# Rust services

Rust versions of the shop services live here, one Cargo crate per service
(for example `services/rust/inventory/`), or a Cargo workspace once there is
more than one.

They talk to the same broker and the same topics as the Python services, so
you can mix and match: run the Python producer with a Rust consumer, or run
a Python and a Rust inventory service side by side in different consumer
groups and compare their output.

The suggested stack is [`rdkafka`](https://docs.rs/rdkafka) (Rust bindings
for librdkafka, the same C library under Python's `confluent-kafka`) with
[Tokio](https://tokio.rs) and `serde_json`. See stage 7 in
[EXERCISES.md](../../EXERCISES.md).
