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

## Home services

`home/` holds the Rust services of the home event hub (`deploy/opsbox`),
split by which way the events flow:

```
home/
  publishers/                 put events INTO Redpanda
    best_share_watch/           a miner beat its own best share (from ckpool)
    block_watch/                our pool found a block (from Bitcoin Core)
  consumers/                  read events OUT of Redpanda
    best_share_alerts/          phone alert for a new best share
    block_alerts/               urgent phone alert for a found block
  shared/                     event types, Redpanda, reaching the Pi, ntfy
```

Each service's `src/main.rs` is short and reads top to bottom; the plumbing
only that service needs sits next to it. They are built and tested in
Docker, so you don't need Rust installed:

```bash
cd deploy/opsbox
docker compose build block-watch      # compiles and runs the tests for all of them
```
