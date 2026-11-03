//! Stage 1: the storefront. Emits OrderPlaced events to the `orders` topic.
//!
//!     cargo run --bin producer                    # one order per second, forever
//!     cargo run --bin producer -- --rate 10       # ten per second
//!     cargo run --bin producer -- --count 20      # send 20 then exit
//!
//! Mirrors `shop/producer.py`. (The `--` separates cargo's own flags from
//! the flags passed to our program.)

use std::time::Duration;

use clap::Parser;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use shop::config::{ORDERS_TOPIC, bootstrap_servers};
use shop::events::{cents, make_order_placed, serialize};

/// Command-line flags. `clap` reads the struct and generates the parser,
/// `--help` included. Python's argparse does the same job.
#[derive(Parser)]
#[command(about = "Emit fake OrderPlaced events to the orders topic")]
struct Args {
    /// Orders per second
    #[arg(long, default_value_t = 1.0)]
    rate: f64,
    /// Stop after N orders (0 = run forever)
    #[arg(long, default_value_t = 0)]
    count: u64,
}

#[tokio::main]
async fn main() -> Result<(), shop::Error> {
    let args = Args::parse();

    // FutureProducer is rdkafka's async-friendly producer: each send gives
    // back a future that resolves when the broker acknowledges the message.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers())
        // Wait for the broker to fully persist each message before acking.
        .set("acks", "all")
        // Retries can't create duplicates when idempotence is on.
        .set("enable.idempotence", "true")
        .create()?;

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    // `interval` ticks at a steady rate. Like `time.sleep(1 / rate)`, but
    // it doesn't drift when sending takes a moment.
    let mut ticker = tokio::time::interval(Duration::from_secs_f64(1.0 / args.rate));

    let mut sent = 0;
    while args.count == 0 || sent < args.count {
        // Wait for the next tick, or stop early on Ctrl+C.
        tokio::select! {
            _ = &mut shutdown => break,
            _ = ticker.tick() => {}
        }

        let order = make_order_placed();
        println!(
            "OrderPlaced {} {:<6} {}",
            order.short_id(),
            order.customer_id,
            cents(order.total_cents)
        );

        let payload = serialize(&order);
        // `FutureRecord::<(), _>` says "this record has no key". Rust needs
        // to know the key's type even when there isn't one.
        //
        // EXERCISE (stage 3): messages with no key are spread across
        // partitions. Add `.key(&order.customer_id)` (and change `()` to
        // `str`) and watch every order for one customer land in the same
        // partition.
        let record = FutureRecord::<(), _>::to(ORDERS_TOPIC).payload(&payload);

        // `send_result` queues the message and returns immediately with a
        // future for the delivery report. It only fails here if the local
        // queue is full.
        let delivery = producer.send_result(record).map_err(|(err, _record)| err)?;

        // Rather than wait for the ack before sending the next order, hand
        // the future to a background task. This is the Rust version of
        // Python's `on_delivery` callback: it runs once the broker has
        // acknowledged (or rejected) the message. Note the partition and
        // offset: an offset is the message's position within its partition.
        tokio::spawn(async move {
            match delivery.await {
                Ok(Ok(d)) => println!(
                    "  -> {ORDERS_TOPIC} partition={} offset={} key=None",
                    d.partition, d.offset
                ),
                Ok(Err((err, _msg))) => println!("  !! delivery failed: {err}"),
                Err(_canceled) => println!("  !! delivery canceled (producer shut down)"),
            }
        });
        sent += 1;
    }

    // flush() blocks until every queued message is delivered. Skip it and
    // the last few orders may never leave your laptop.
    println!("flushing...");
    producer.flush(Duration::from_secs(10))?;
    // Give the spawned tasks a moment to print their last delivery lines.
    tokio::time::sleep(Duration::from_millis(100)).await;
    Ok(())
}
