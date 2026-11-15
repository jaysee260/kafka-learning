//! Stage 1: the simplest possible consumer. Prints every order.
//!
//!     cargo run --bin order_logger
//!
//! Mirrors `shop/consumers/order_logger.py`. The consume loop is written
//! out in full here so you can see every step. The stage 2 services reuse
//! the same loop via `shop::consumer::run_consumer` (src/consumer.rs).

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use shop::config::{ORDERS_TOPIC, bootstrap_servers};
use shop::events::{cents, deserialize};

// `#[tokio::main]` starts Tokio's async runtime and runs `main` on it. That
// lets us `.await` the next message and Ctrl+C at the same time.
//
// Returning `Result` means any `?` below that hits an error ends the
// program with that error printed, like an uncaught exception in Python.
#[tokio::main]
async fn main() -> Result<(), shop::Error> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers())
        // The consumer group. Kafka stores how far each group has read
        // (its committed offsets) so a restart picks up where it left off.
        .set("group.id", "order-logger")
        // Only used when the group has NO committed offset yet (first run):
        // "earliest" = start from the oldest retained message,
        // "latest"   = only see messages produced from now on.
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer.subscribe(&[ORDERS_TOPIC])?;
    println!("order-logger reading '{ORDERS_TOPIC}' (Ctrl+C to stop)");

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        // Wait for whichever comes first: Ctrl+C, or the next message.
        // Python polls with a 1s timeout and gets None when nothing arrived;
        // `recv()` simply waits, so there's no "nothing arrived" case.
        let msg = tokio::select! {
            _ = &mut shutdown => break,
            result = consumer.recv() => result?,
        };

        // `payload()` is an Option because a Kafka message may have no value.
        // If it isn't valid JSON for an OrderPlaced, stop with a message that
        // says where the bad message is. (Stage 4 is about doing better.)
        let order = deserialize(msg.payload().unwrap_or_default())
            .map_err(|err| format!("can't parse message at p{} @{}: {err}", msg.partition(), msg.offset()))?;
        println!(
            "[p{} @{}] {:<6} ordered {} item(s), total {}",
            msg.partition(),
            msg.offset(),
            order.customer_id,
            order.items.len(),
            cents(order.total_cents),
        );
        // Offsets are committed automatically in the background every few
        // seconds (enable.auto.commit defaults to true).
    }

    // Leaving the loop drops `consumer`, which closes it: it commits final
    // offsets and tells the group we're leaving, so partitions are handed
    // to other members right away. Same as `consumer.close()` in Python.
    Ok(())
}
