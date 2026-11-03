# kafka-learning

A hands-on project for learning Kafka by building an order-processing
pipeline for a tiny fake e-commerce shop. It runs on
[Redpanda](https://redpanda.com), which speaks the Kafka protocol, so
everything you learn here (clients, topics, partitions, consumer groups,
offsets) applies to Apache Kafka unchanged.

```
                          ┌──▶ inventory       decrements stock
┌──────────┐    orders    │
│ producer │ ─────────────┼──▶ notifications   "emails" the customer
└──────────┘    (topic)   │
                          └──▶ analytics       running revenue totals
```

Stages 1 and 2 are built and working. Stages 3 to 7 are exercises in
[EXERCISES.md](EXERCISES.md) that you build on top of this code.

| Stage | What you learn | Status |
| --- | --- | --- |
| 1. Producer + one consumer | topics, serialization, offsets | built |
| 2. Fan-out | consumer groups as independent readers | built |
| 3. Partitions and keys | ordering, scaling a group, rebalancing | exercise |
| 4. Failure and replay | at-least-once, offset resets, dead-letter topics | exercise |
| 5. Stream processing | joins, windowed aggregation | exercise |
| 6. Kafka Connect + dashboard (stretch) | integrating with databases | exercise |
| 7. Rewrite inventory in Rust | `rdkafka` + Tokio, polyglot services | exercise |

## The core ideas in five minutes

**Topic.** A named, append-only log of messages. Producers add to the end;
nothing is modified in place. Messages are not deleted when read: they stay
until the topic's retention period expires (7 days by default). That is the
big difference from a traditional queue.

**Message.** A key (optional), a value, a timestamp and headers. To the
broker the key and value are just bytes; *you* choose the serialization
(here, JSON).

**Partition.** Each topic is split into partitions, and each partition is
its own ordered log. Ordering is guaranteed *within* a partition, not across
the whole topic. Messages with the same key always go to the same partition.

**Offset.** A message's position in its partition: 0, 1, 2, ... A consumer
tracks progress as "the next offset I need in each partition."

**Consumer group.** A set of consumer instances sharing a `group.id`. Kafka
splits the topic's partitions among the group's members, and remembers each
group's committed offsets. Two *different* groups each get the full stream
independently, which is how one `orders` topic feeds three services that
don't know about each other.

**Broker.** A server that stores partitions and serves clients. Here there
is one (the `redpanda` container). In production you run several and
replicate each partition across them.

## Setup

You need Docker (with Compose) and Python 3.10 or newer.

```bash
# 1. Start Redpanda and Redpanda Console
docker compose up -d
docker compose ps            # wait until redpanda shows "healthy"

# 2. Set up Python
cd services/python
python3 -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 3. Create the topics (orders has 3 partitions)
python -m shop.create_topics
```

Open **Redpanda Console at http://localhost:8080** and keep it open. It
shows topics, individual messages with their partition and offset, and
consumer groups with their lag. It's the best way to see what's happening.

All Python commands below are run from `services/python` with the virtualenv
active. The broker address defaults to `localhost:19092`; override it with
`BOOTSTRAP_SERVERS` if needed.

You can also poke at the broker from the command line with `rpk`, Redpanda's
CLI, which is inside the container:

```bash
docker exec -it redpanda rpk topic list
docker exec -it redpanda rpk topic consume orders --num 3
docker exec -it redpanda rpk group list
docker exec -it redpanda rpk group describe inventory
```

## Stage 1: producer and one consumer

Terminal 1, the storefront, placing one order per second:

```bash
python -m shop.producer
```

Each line it prints is followed by a delivery report like
`-> orders partition=1 offset=4`. That's the broker confirming where the
message was stored.

Terminal 2, a consumer that prints every order:

```bash
python -m shop.consumers.order_logger
```

Things to try:

1. Stop the logger (Ctrl+C), let the producer run for a while, then start
   the logger again. It catches up on everything it missed, because its
   group (`order-logger`) has a committed offset and the messages are still
   on disk.
2. In Console, open the `orders` topic and find the same messages. Note the
   partition and offset of each.
3. In Console, open Consumer Groups → `order-logger` and watch the *lag*
   (how far behind the newest message the group is) while the logger is
   stopped and after it restarts.

Code to read: [`producer.py`](services/python/shop/producer.py) and
[`order_logger.py`](services/python/shop/consumers/order_logger.py). Every
config option has a comment explaining it.

## Stage 2: fan-out to independent services

Keep the producer running and start each service in its own terminal:

```bash
python -m shop.consumers.inventory
python -m shop.consumers.notifications
python -m shop.consumers.analytics
```

Each one uses a different `group.id`, so each receives *every* order and
tracks its own offsets. The producer has no idea they exist. Adding a fourth
service (fraud checks, loyalty points, ...) means writing a new consumer,
with no change to the producer or the other services. That decoupling is the
main reason teams reach for Kafka.

Things to try:

1. Stop analytics for a minute while the others keep going. In Console, see
   that only the `analytics` group has lag. Restart it and watch it catch up
   without affecting anyone else.
2. Run `python -m shop.producer --count 50 --rate 20` with all services
   stopped, then start them one at a time.

The shared poll loop is in [`consumer.py`](services/python/shop/consumer.py).
It stores an offset only after the handler succeeds, which gives you
at-least-once processing; stage 4 explains why that matters.

Always stop consumers with **Ctrl+C**. That runs `consumer.close()`, which
commits offsets and tells the group the member is leaving. If a consumer is
killed instead, the group waits for its session timeout (45 seconds by
default) before it hands that member's partitions to anyone else.

## Repo layout

```
docker-compose.yml        Redpanda broker + Redpanda Console
EXERCISES.md              stages 3 to 7
services/
  python/
    requirements.txt
    shop/
      config.py           broker address and topic names
      events.py           OrderPlaced shape, fake data, JSON (de)serialization
      create_topics.py    creates orders, payments, orders.dlq
      producer.py         stage 1: emits OrderPlaced
      consumer.py         shared consume loop for stage 2 services
      send_bad_message.py stage 4 helper: publishes a malformed message
      consumers/
        order_logger.py   stage 1
        inventory.py      stage 2
        notifications.py  stage 2
        analytics.py      stage 2
  rust/                   Rust services (stage 7)
```

Services in different languages share one broker and one set of topics, so
a Rust consumer can read what the Python producer writes.

## Resetting

```bash
docker compose down -v     # stops containers and deletes all topic data
docker compose up -d
python -m shop.create_topics
```

## Further reading

- [Kafka: The Definitive Guide](https://www.confluent.io/resources/kafka-the-definitive-guide-v2/) (free from Confluent), chapters 1 to 4 cover everything in stages 1 to 4.
- [Redpanda docs: rpk](https://docs.redpanda.com/current/reference/rpk/)
- [confluent-kafka Python API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
- [librdkafka configuration reference](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md), the options shared by the Python and Rust clients.
