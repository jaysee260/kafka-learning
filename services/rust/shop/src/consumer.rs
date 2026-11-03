//! A reusable consume loop for the stage 2 services.
//!
//! Mirrors `services/python/shop/consumer.py`. Each service is just a group
//! id plus a function that handles one event:
//!
//! ```ignore
//! run_consumer("inventory", |order, msg| { /* ... */ }).await
//! ```
//!
//! Every distinct group id gets its OWN copy of the stream with its own
//! offsets. That is what lets inventory, notifications and analytics all
//! read the same `orders` topic without knowing about each other.
//!
//! Because these settings are the same librdkafka settings that Python's
//! `confluent-kafka` uses, a Rust service and a Python service can even
//! join the SAME group and split the partitions between them.

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::{ClientContext, TopicPartitionList};

use crate::Error;
use crate::config::{ORDERS_TOPIC, bootstrap_servers};
use crate::events::{OrderPlaced, deserialize};

/// Consume `orders` as part of consumer group `group_id`, calling `handle`
/// once for every event, until Ctrl+C.
///
/// About the `handle` parameter:
///
/// - `impl FnMut(...)` means "any function or closure with this signature".
///   Python's `Callable[[dict, Message], None]`, roughly.
/// - `FnMut` (rather than `Fn`) means the closure is allowed to change the
///   variables it captured. That's how inventory and analytics keep their
///   running totals: the state lives in the closure, not in globals.
/// - It receives a borrowed `&OrderPlaced` (already parsed and validated)
///   and the raw message, for its partition and offset.
pub async fn run_consumer(
    group_id: &str,
    mut handle: impl FnMut(&OrderPlaced, &BorrowedMessage<'_>),
) -> Result<(), Error> {
    let consumer: StreamConsumer<LoggingContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers())
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        // At-least-once processing:
        // The background committer only commits offsets we have explicitly
        // "stored". We store an offset only AFTER handle() returns, so a
        // crash mid-message means it is redelivered, never silently skipped.
        // The trade-off is a message can be processed twice (stage 4).
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false")
        // During a rebalance, only the partitions that actually move are
        // revoked, instead of pausing the whole group. All members of a
        // group must agree on this, so it has to match consumer.py for a
        // Rust and a Python instance to share a group.
        .set("partition.assignment.strategy", "cooperative-sticky")
        // `create_with_context` builds the consumer and attaches our
        // LoggingContext (defined below), which prints rebalances.
        .create_with_context(LoggingContext {
            group_id: group_id.to_string(),
        })?;

    consumer.subscribe(&[ORDERS_TOPIC])?;
    println!("[{group_id}] started (Ctrl+C to stop)");

    // A future that completes when you press Ctrl+C. `pin!` lets us poll the
    // same future on every pass through the loop instead of making a new one.
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        // `select!` waits on several things at once and runs the branch of
        // whichever is ready first. Here: "Ctrl+C was pressed" or "a message
        // arrived". This replaces Python's `while True: poll(1.0)` plus
        // `except KeyboardInterrupt`, with no timeout and no busy looping.
        tokio::select! {
            _ = &mut shutdown => break,

            result = consumer.recv() => {
                // `recv()` returns Err for client errors. Like consumer.py,
                // treat them as fatal: `?` returns the error from this
                // function, and main() prints it and exits.
                let msg = result?;

                // A message with no value (a "tombstone") is legal in Kafka
                // but isn't an order. Treat it as empty bytes, which then
                // fails to parse like any other malformed message.
                let payload = msg.payload().unwrap_or_default();

                // EXERCISE (stage 4 / stage 7 task 5): a malformed message
                // makes this `?` stop the service. Restart it and it stops
                // again on the same message, because its offset was never
                // stored. Unlike Python, the failure is visible in the types:
                // replace the `?` with a `match`, and in the `Err` arm send
                // the raw bytes to ORDERS_DLQ_TOPIC with a FutureProducer
                // (see src/bin/send_bad_message.rs for how to produce).
                let order = deserialize(payload).map_err(|err| {
                    format!("can't parse message at p{} @{}: {err}", msg.partition(), msg.offset())
                })?;
                handle(&order, &msg);

                // Done with this message: mark its offset for the background
                // committer (every auto.commit.interval.ms, 5s by default).
                consumer.store_offset_from_message(&msg)?;
            }
        }
    }

    // Returning drops `consumer`, and dropping it closes it: rdkafka commits
    // the stored offsets and sends a LeaveGroup request, so the rest of the
    // group gets this instance's partitions right away instead of waiting
    // for the session timeout (45s) to notice it's gone. That is what
    // `consumer.close()` does in Python's `finally:` block.
    println!("[{group_id}] shutting down");
    Ok(())
}

// ---------------------------------------------------------------------------
// Rebalance logging
// ---------------------------------------------------------------------------
//
// rdkafka lets you attach a "context" to a client: a struct whose trait
// methods are called on client events. It's the Rust equivalent of passing
// `on_assign=` and `on_revoke=` to `subscribe()` in consumer.py.
//
// rdkafka does the actual (incremental) assign/unassign itself and calls
// `pre_rebalance` before and `post_rebalance` after. We only print. Watch
// these lines while you start and stop copies of a service (stage 3).

struct LoggingContext {
    group_id: String,
}

// Every context must implement ClientContext (logging, stats, errors).
// The default methods are fine, so the block is empty.
impl ClientContext for LoggingContext {}

impl ConsumerContext for LoggingContext {
    fn pre_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Revoke(partitions) = rebalance {
            println!(
                "[{}] revoked partitions: {:?}",
                self.group_id,
                partition_numbers(partitions)
            );
        }
    }

    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(partitions) => {
                println!(
                    "[{}] assigned partitions: {:?}",
                    self.group_id,
                    partition_numbers(partitions)
                );
            }
            Rebalance::Error(err) => eprintln!("[{}] rebalance error: {err}", self.group_id),
            Rebalance::Revoke(_) => {} // already printed in pre_rebalance
        }
    }
}

/// The sorted partition numbers in a list, e.g. `[0, 2]`.
fn partition_numbers(list: &TopicPartitionList) -> Vec<i32> {
    let mut numbers: Vec<i32> = list.elements().iter().map(|tp| tp.partition()).collect();
    numbers.sort();
    numbers
}
