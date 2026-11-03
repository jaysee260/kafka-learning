# Rust services

Rust versions of every Python service in this repo. They use the same
broker, the same topics and the same JSON, so you can mix and match freely:
the Python producer feeding Rust consumers, a Rust producer feeding Python
consumers, or a Python and a Rust inventory service sharing one consumer
group.

The stack is [`rdkafka`](https://docs.rs/rdkafka) (Rust bindings for
librdkafka, the same C library under Python's `confluent-kafka`, so every
config key is identical), [Tokio](https://tokio.rs) for async, and
[`serde`](https://serde.rs) for JSON.

## Python to Rust map

Everything lives in one Cargo package, `shop/`, laid out like the Python
`shop` package. Shared code is a library (`src/lib.rs` and friends); each
service is a small program in `src/bin/`.

| Python (`services/python/shop/`) | Rust (`services/rust/shop/src/`) | Run with |
| --- | --- | --- |
| `config.py` | `config.rs` | (library) |
| `events.py` | `events.rs` | (library) |
| `consumer.py` | `consumer.rs` | (library) |
| `create_topics.py` | `bin/create_topics.rs` | `cargo run --bin create_topics` |
| `producer.py` | `bin/producer.rs` | `cargo run --bin producer` |
| `send_bad_message.py` | `bin/send_bad_message.rs` | `cargo run --bin send_bad_message` |
| `consumers/order_logger.py` | `bin/order_logger.rs` | `cargo run --bin order_logger` |
| `consumers/inventory.py` | `bin/inventory.rs` | `cargo run --bin inventory` |
| `consumers/notifications.py` | `bin/notifications.rs` | `cargo run --bin notifications` |
| `consumers/analytics.py` | `bin/analytics.rs` | `cargo run --bin analytics` |

Each file has a comment at the top saying which Python file it mirrors, and
comments throughout explain the Rust parts that have no Python equivalent.

### Suggested reading order

1. `events.rs`: the `OrderPlaced` struct and how serde turns JSON into it.
   This is the biggest difference from Python, where an event is a dict.
2. `bin/producer.rs`: producing, plus Tokio's `select!` and `spawn`.
3. `bin/order_logger.rs`: the consume loop written out in full.
4. `consumer.rs`: the same loop, reusable, with at-least-once offsets and
   rebalance logging.
5. `bin/inventory.rs`, `bin/notifications.rs`, `bin/analytics.rs`: each is
   just a closure passed to `run_consumer`. Compare how inventory and
   analytics keep state in a closure instead of in globals.

## Setup

You need the broker running and the topics created (see the main
[README](../../README.md)), plus:

- **Rust**, installed with [rustup](https://rustup.rs). Any recent stable
  toolchain works (Rust 1.87 or newer).
- **A C toolchain**, because `rdkafka` compiles librdkafka from source on
  the first build. On macOS that's `xcode-select --install`; on Debian or
  Ubuntu, `sudo apt install build-essential`.

```bash
cd services/rust/shop
cargo build          # first build takes a minute or two (it compiles librdkafka)
cargo test           # a few unit tests for the JSON model and money formatting
```

All Rust commands below are run from `services/rust/shop`. Like the Python
services, they connect to `localhost:19092` unless you set
`BOOTSTRAP_SERVERS`.

`cargo run --bin NAME` builds (if needed) and runs one service. Flags for
the program go after `--`, for example `cargo run --bin producer -- --rate 5`.

## Stage 1 in Rust

```bash
cargo run --bin create_topics       # same as python -m shop.create_topics, safe to re-run
cargo run --bin producer            # terminal 1: one order per second
cargo run --bin order_logger        # terminal 2: prints every order
```

The output matches the Python versions line for line. Try the stage 1
exercises from the main README with these instead.

## Stage 2 in Rust

Each in its own terminal, with a producer (either language) running:

```bash
cargo run --bin inventory
cargo run --bin notifications
cargo run --bin analytics
```

These use the same group ids as the Python services (`inventory`,
`notifications`, `analytics`). That matters, see below.

## Mixing Python and Rust

This is the point of stage 7: Kafka doesn't care what language a client is
written in.

**One group, two languages.** Start the Python inventory service, then the
Rust one:

```bash
# terminal 1 (services/python, venv active)
python -m shop.consumers.inventory

# terminal 2 (services/rust/shop)
cargo run --bin inventory
```

Both join the group `inventory`, so the group splits the 3 partitions
between them. You'll see Python print `revoked partitions: [0]` and Rust
print `assigned partitions: [0]`. Start a producer and each order is
handled by exactly one of them. Press Ctrl+C on the Rust one and Python
immediately gets partition 0 back.

(This only works because both use `partition.assignment.strategy =
cooperative-sticky`. Members of one group must agree on the strategy.)

**Two groups, every order twice.** Give the Rust service its own group:

```bash
GROUP_ID=inventory-rust cargo run --bin inventory
```

Now it's an independent reader, so it and the Python service each see every
order and each keep their own stock count. In Redpanda Console you'll see
both groups with their own offsets and lag.

## Stopping cleanly

Always use **Ctrl+C**. Every Rust consumer waits on `tokio::signal::ctrl_c()`
alongside the next message, breaks out of its loop, and drops the consumer.
Dropping it closes it: final offsets are committed and the group is told the
member is leaving, so its partitions move to another member right away
instead of after the 45 second session timeout.

## What's left as an exercise

- **Dead-letter topic (stage 4 / stage 7 task 5).** Run
  `cargo run --bin send_bad_message` and every Rust consumer stops with
  `can't parse message at pN @M`, just like the Python ones. Fix it in
  `consumer.rs`, where the `EXERCISE` comment marks the spot.
- **Keys (stage 3).** The `EXERCISE` comment in `bin/producer.rs` shows where
  to key orders by customer.
