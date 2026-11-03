//! Shared settings: where the broker lives and what the topics are called.
//!
//! Mirrors `services/python/shop/config.py`, so Python and Rust services
//! always agree on topic names. Override the broker address with the
//! `BOOTSTRAP_SERVERS` environment variable.

/// The "external" listener from docker-compose.yml. A client only needs one
/// broker address to start; it discovers the rest of the cluster from there.
const DEFAULT_BOOTSTRAP_SERVERS: &str = "localhost:19092";

/// Stage 1+: OrderPlaced events.
pub const ORDERS_TOPIC: &str = "orders";
/// Stage 5: PaymentReceived events.
pub const PAYMENTS_TOPIC: &str = "payments";
/// Stage 4: messages we could not process.
pub const ORDERS_DLQ_TOPIC: &str = "orders.dlq";

/// How many partitions `orders` gets. More partitions = more consumers can
/// share the work in one consumer group (stage 3).
pub const ORDERS_PARTITIONS: i32 = 3;

/// The broker address: `$BOOTSTRAP_SERVERS` if set, otherwise localhost.
///
/// This is a function rather than a constant because reading an
/// environment variable happens at run time, and Rust constants must be
/// known at compile time.
pub fn bootstrap_servers() -> String {
    std::env::var("BOOTSTRAP_SERVERS").unwrap_or_else(|_| DEFAULT_BOOTSTRAP_SERVERS.to_string())
}
