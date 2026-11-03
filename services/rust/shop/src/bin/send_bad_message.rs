//! Stage 4 helper: publish one malformed message to `orders`.
//!
//!     cargo run --bin send_bad_message
//!
//! Mirrors `shop/send_bad_message.py`. Use it to see what a "poison pill"
//! does to the consumers (Python and Rust alike), then build the
//! dead-letter topic handling described in EXERCISES.md.

use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use shop::config::{ORDERS_TOPIC, bootstrap_servers};

#[tokio::main]
async fn main() -> Result<(), shop::Error> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers())
        .create()?;

    let record = FutureRecord::<(), _>::to(ORDERS_TOPIC).payload("{this is not valid json");

    // `send` waits (up to the timeout) for room in the local queue, then for
    // the broker's ack. On failure it returns the error and the message.
    producer
        .send(record, Duration::from_secs(10))
        .await
        .map_err(|(err, _msg)| err)?;

    println!("sent a malformed message to '{ORDERS_TOPIC}'");
    Ok(())
}
