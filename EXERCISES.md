# Exercises: stages 3 to 7

Stages 1 and 2 are built (see the [README](README.md)). Each stage below
explains a concept, gives you tasks, and lists questions to check your
understanding. Work through them in order; each builds on the last.

All Python commands run from `services/python` with the virtualenv active.

---

## Stage 3: partitions, keys and scaling a consumer group

### The concept

A topic's partitions are the unit of parallelism. Within one consumer group,
**each partition is read by exactly one member at a time**. So:

- 3 partitions, 1 member: that member reads all 3.
- 3 partitions, 2 members: one gets 2, the other gets 1.
- 3 partitions, 4 members: one member sits idle. Partition count caps a
  group's parallelism.

When members join or leave, the group **rebalances**: partitions are
reassigned among the members that are left.

Ordering is per partition. If you need all events for one customer handled
in order (say, "order placed" before "order cancelled"), they must land in
the same partition. You get that by giving them the same **key**: the
producer hashes the key to pick the partition.

### Tasks

1. **See unkeyed spreading.** Run the producer as is and watch the delivery
   reports. Orders for `alice` land in every partition.
2. **Key by customer.** In [`producer.py`](services/python/shop/producer.py),
   change `key=None` to `key=order["customer_id"]`. Restart the producer. Now
   every order for a given customer goes to the same partition. Confirm it in
   the delivery reports and in Console.
3. **Scale out.** Start two copies of inventory in separate terminals. Both
   use group id `inventory`, so they share the work. Look at the
   `assigned partitions:` lines each prints, and in Console under Consumer
   Groups → `inventory`.
4. **Rebalance.** Ctrl+C one of the two. Watch the survivor get its
   partitions. Start it again and watch them split back.
5. **Too many consumers.** Start four copies. One should be assigned
   nothing.
6. **Spot the bug.** With two inventory instances running, each keeps its own
   in-memory `stock` dict. Neither has the true stock level. Write down why,
   and how you would fix it (hint: where should shared state live?).

### Check your understanding

- Why does adding partitions later break the "same key, same partition"
  guarantee for existing keys?
- Why can a hot key (one customer placing 90% of orders) be a problem?
- If you need strict global ordering across all orders, what partition count
  do you need, and what does that cost you?

---

## Stage 4: failure, replay and dead letters

### The concept

A consumer **commits** offsets to record its progress. When it restarts, it
resumes from the last committed offset. When you commit relative to doing
the work decides your guarantee:

- **At-most-once:** commit, then process. A crash in between loses the
  message.
- **At-least-once:** process, then commit. A crash in between means the
  message is processed again. This is what `consumer.py` does, and it is
  the usual choice, so handlers should be **idempotent** (safe to run
  twice).
- **Exactly-once:** possible with Kafka transactions when both input and
  output are Kafka topics. Worth reading about after this stage.

Because messages stay on disk, you can also **rewind** a group to rebuild
state from history. This is called replay.

A **poison pill** is a message a consumer can never process (malformed,
violates an assumption). Without handling, it blocks its partition forever:
crash, restart, same message, crash. The standard fix is a **dead-letter
topic (DLQ)**: catch the failure, publish the original message there with
error details, and move on.

### Tasks

1. **Resume after a crash.** Run analytics, let it count 30 orders, then
   kill it hard (close the terminal, or `kill -9`). Restart it. Its totals
   restart from zero but it doesn't reprocess old orders, because it
   resumes from its committed offset (after up to 45 seconds while the group
   notices the old member is gone). Notice the totals are now wrong. Keep
   that in mind for task 3.
2. **Duplicates.** In `consumer.py`, add `time.sleep(3)` and a print right
   before `consumer.store_offsets(...)`. Kill the process during the sleep and
   restart. The same message is handled twice. Think about what that means
   for the notifications service.
3. **Replay to rebuild state.** Stop analytics, then reset its offsets to
   the beginning and restart it:
   ```bash
   docker exec -it redpanda rpk group seek analytics --to start
   python -m shop.consumers.analytics
   ```
   It recomputes the totals from the full history. This is the power of a
   retained log: state is derived, so you can always rebuild it. Also try
   `--to end` and `--to <timestamp>`.
4. **Poison pill.** With inventory running, send a malformed message:
   ```bash
   python -m shop.send_bad_message
   ```
   Inventory crashes. Restart it: it crashes again on the same message.
5. **Build the DLQ.** In `consumer.py`, wrap `deserialize()` and `handle()` in
   `try/except`. On failure, produce the original `msg.value()` and
   `msg.key()` to `orders.dlq`, with headers such as `error`, `source.topic`,
   `source.partition` and `source.offset`. Then store the offset and move on.
   Verify in Console that the bad message is in `orders.dlq` and inventory
   keeps going.
6. **Idempotency (bonus).** Make notifications skip orders it has already
   emailed by remembering `order_id`s. What breaks when it restarts? Where
   would that memory live in a real system?

### Check your understanding

- Why is "process then commit" safer than "commit then process" for an
  inventory service?
- What happens to a partition's other messages while a poison pill blocks
  it?
- When should a DLQ'd message be retried, and by whom?

---

## Stage 5: stream processing

### The concept

So far each consumer handles one message at a time and forgets it. Stream
processing keeps state across messages to **join** streams or **aggregate
over time windows**.

- **Join:** combine `OrderPlaced` from `orders` with `PaymentReceived` from
  `payments` (same `order_id`) to emit `OrderConfirmed`. The two can arrive
  in either order and seconds apart, so you buffer whichever arrives first.
- **Windowing:** group events into time buckets. A **tumbling** window is
  fixed and non-overlapping ("revenue per minute"). A **hopping** or
  **sliding** window overlaps ("revenue in the last 5 minutes, updated every
  minute").
- **Event time vs processing time:** use the time the order was *placed*
  (`placed_at`), not the time you happened to read it, or replays and lag
  will put orders in the wrong window.

Dedicated frameworks (Kafka Streams in Java, Apache Flink, Faust or
Bytewax in Python) handle state, windows and late data for you. Doing it by
hand first shows you what they solve.

### Tasks

1. **Payments producer.** Write `shop/payments_producer.py` that consumes
   `orders` and, for about 90% of orders, waits a random 0 to 5 seconds and
   produces a `PaymentReceived` event (`order_id`, `amount_cents`,
   `paid_at`) to `payments`. Key it by `customer_id` like orders.
2. **Join.** Write `shop/consumers/order_confirmer.py`, subscribed to both
   `orders` and `payments` (`run_consumer` accepts a `topics` list; you can
   tell them apart with `msg.topic()`). Keep a dict of unmatched orders and
   unmatched payments by `order_id`. When both sides are present, produce
   `OrderConfirmed` to a new `orders.confirmed` topic.
3. **Expire.** Orders with no payment after 30 seconds should produce
   `OrderPaymentTimedOut` instead. Bound the buffers so memory can't grow
   forever.
4. **Window.** Write `shop/consumers/revenue_per_minute.py`, which buckets
   orders by the minute of `placed_at` and prints each minute's revenue
   once the minute has closed. Decide how long to wait for late events
   before closing a window.
5. **Co-partitioning question.** If you ran two instances of the confirmer,
   why must `orders` and `payments` have the same partition count and the
   same key?

### Check your understanding

- What happens to your in-memory join buffers when the confirmer restarts?
  How do frameworks avoid losing them? (Look up "changelog topics.")
- Why does replaying `orders` from the start give different window results
  with processing time but the same results with event time?

---

## Stage 6 (stretch): Kafka Connect and a dashboard

### The concept

**Kafka Connect** moves data between Kafka and other systems with
configuration instead of code: *source* connectors bring data in (for
example, change data capture from a database), *sink* connectors push it out
(to Postgres, S3, Elasticsearch). Redpanda offers **Redpanda Connect**
(formerly Benthos), a single binary configured with YAML, which is the
easiest way to try this here. Apache Kafka Connect with the JDBC sink is the
classic alternative.

### Tasks

1. Add a `postgres` service to `docker-compose.yml`.
2. Add a Redpanda Connect (or Kafka Connect) service that reads
   `orders.confirmed` and inserts rows into an `orders` table.
3. Build a tiny dashboard (Streamlit, Flask, or anything you like) that
   queries Postgres for revenue per minute and top products.
4. Reverse direction (bonus): use change data capture (Debezium, or
   Redpanda Connect's Postgres CDC input) so a row inserted in a `refunds`
   table appears as an event on a `refunds` topic.

---

## Stage 7: rewrite the inventory service in Rust

### The concept

Kafka is language-neutral: the broker only sees bytes on topics. Services in
different languages interoperate as long as they agree on the message
format. That's a big part of why it's used between teams.

Rust's main client is [`rdkafka`](https://docs.rs/rdkafka), which wraps
librdkafka, the same C library under Python's `confluent-kafka`. So the
config keys you already know (`bootstrap.servers`, `group.id`,
`enable.auto.offset.store`, ...) carry over exactly. Its `StreamConsumer`
fits naturally with [Tokio](https://tokio.rs) async code.

### What's built

Rust versions of every service are in [`services/rust/shop`](services/rust/shop),
with a walkthrough in [`services/rust/README.md`](services/rust/README.md):
`OrderPlaced` and `Item` structs with `#[derive(Deserialize)]`, a
`StreamConsumer` with the same at-least-once settings as `consumer.py`, the
same output as the Python services, and clean shutdown on Ctrl+C with
`tokio::signal::ctrl_c()`.

### Tasks

1. Build it and read the code in the order the Rust README suggests.
   Compare each file with the Python file it mirrors.
2. Run the Rust inventory service alongside the Python one. First with the
   default group id `inventory`, and watch the Python and Rust instances
   split the partitions between them (one group, two languages). Then with
   `GROUP_ID=inventory-rust`, and watch both independently process every
   order.
3. Feed Python consumers from the Rust producer, and Rust consumers from the
   Python producer.
4. Handle a malformed message the Rust way: deserialization returns a
   `Result`, so in `consumer.rs` route the `Err` case to `orders.dlq`
   instead of stopping. Test it with `cargo run --bin send_bad_message`.
5. Do the stage 3 key exercise in `bin/producer.rs`.
6. Write a new Rust-only service, for example a fraud checker that flags
   orders over $200, using `run_consumer` and a new group id.

### Check your understanding

- What would break if the Python producer renamed `total_cents` to `total`?
  How do schemas (Avro or Protobuf with Schema Registry, which Redpanda
  includes on port 18081) prevent that?
- What are the trade-offs of keeping event definitions in one shared place
  versus per service?
