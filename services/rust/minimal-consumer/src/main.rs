//! The simplest Rust service that processes Kafka messages.
//!
//! It does four things, and that is the whole lifecycle of a consumer:
//!
//!   1. connect   -> build a consumer pointed at the broker
//!   2. subscribe -> say which topic(s) we want
//!   3. loop      -> wait for the next message, forever
//!   4. process + commit -> handle the message, THEN tell Kafka "done"
//!
//! Step 4 is the interesting one. Kafka does not delete a message when you
//! read it. Instead, each consumer group keeps a bookmark per partition,
//! called the "committed offset": the position of the next message the group
//! has NOT yet processed. Committing moves the bookmark forward. If this
//! service crashes and restarts, it resumes from the bookmark.
//!
//! Here we commit by hand, after processing, so you can see exactly when a
//! message counts as "processed". (The services in ../shop let rdkafka commit
//! in the background instead. Same idea, less code to look at.)
//!
//! Try it:
//!   docker compose up -d                       # from the repo root
//!   cargo run                                  # from this folder
//!   # in another terminal, send some orders with the Python or Rust producer
//!
//! Then stop it with Ctrl+C, start it again, and notice it does NOT reprint
//! old messages: its group's bookmark is already past them. Change GROUP_ID
//! below to a new name and it reads everything from the start again, because
//! a new group has no bookmark yet.

use rdkafka::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;

/// Where the broker is. Redpanda in docker-compose.yml listens here.
const BROKERS: &str = "localhost:19092";
/// The topic the shop producer writes orders to.
const TOPIC: &str = "orders";
/// Our consumer group's name. Kafka stores committed offsets per group, so
/// this name IS the identity of "how far have we got".
const GROUP_ID: &str = "minimal-consumer";

// `#[tokio::main]` starts the Tokio runtime and runs our async `main` on it.
// Returning `Result` lets us use `?`: any error ends the program and is
// printed, which is fine for a learning service.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ---- 1. Connect ------------------------------------------------------
    // ClientConfig is a bag of librdkafka settings (string key, string
    // value). These are the same keys you'd use from Python or Java.
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("group.id", GROUP_ID)
        // A brand-new group has no bookmark. "earliest" means: then start
        // from the oldest message still in the topic. (The default,
        // "latest", would only show messages sent after we start.)
        .set("auto.offset.reset", "earliest")
        // Turn OFF automatic commits. Now nothing is marked as processed
        // unless we call commit_message() ourselves, below.
        .set("enable.auto.commit", "false")
        .create()?;

    // ---- 2. Subscribe ----------------------------------------------------
    // We name the topic, not the partitions. The group coordinator (on the
    // broker) decides which of the topic's partitions this instance gets.
    // Start a second copy of this program and the partitions get split
    // between the two: that's how consumers scale out.
    consumer.subscribe(&[TOPIC])?;
    println!("listening on '{TOPIC}' as group '{GROUP_ID}' (Ctrl+C to stop)");

    // ---- 3. Loop ---------------------------------------------------------
    loop {
        // Wait (without burning CPU) until the next message arrives.
        // `msg` borrows from the consumer, so it lives only for this pass.
        let msg = consumer.recv().await?;

        // ---- 4a. Process -------------------------------------------------
        // Every message has a value (the payload, raw bytes), an optional
        // key, and its address: which partition it's in, and its offset
        // (position) within that partition.
        let value = match msg.payload_view::<str>() {
            Some(Ok(text)) => text,
            Some(Err(_)) => "<not valid UTF-8>",
            None => "<empty>",
        };
        println!(
            "partition {} offset {}: {value}",
            msg.partition(),
            msg.offset()
        );

        // This is where real work would go: update a database, call an API,
        // produce a new event to another topic...

        // ---- 4b. Commit --------------------------------------------------
        // Only now do we say "done with this one". This stores offset + 1
        // (the NEXT message to read) as the group's bookmark for this
        // partition.
        //
        // Because we commit AFTER processing, a crash between 4a and 4b
        // means the message is read again on restart. That's called
        // "at-least-once" delivery: nothing is lost, but something may be
        // processed twice. Commit BEFORE processing and you flip it to
        // "at-most-once": never twice, but a crash can lose a message.
        //
        // CommitMode::Sync waits for the broker to confirm. It's the easiest
        // to reason about, but one round trip per message is slow. Real
        // services use CommitMode::Async, or commit every N messages.
        consumer.commit_message(&msg, CommitMode::Sync)?;
    }
}
