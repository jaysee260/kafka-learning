"""Create the topics this project uses.

Brokers can auto-create topics on first write, but then you get default
settings (often 1 partition). Creating them explicitly lets us choose the
partition count, which matters for stage 3.

    python -m shop.create_topics
"""

from confluent_kafka.admin import AdminClient, NewTopic

from shop.config import (
    BOOTSTRAP_SERVERS,
    ORDERS_DLQ_TOPIC,
    ORDERS_PARTITIONS,
    ORDERS_TOPIC,
    PAYMENTS_TOPIC,
)

TOPICS = [
    # replication_factor=1 because we run a single broker. In production
    # you would use 3 so a broker can die without losing data.
    NewTopic(ORDERS_TOPIC, num_partitions=ORDERS_PARTITIONS, replication_factor=1),
    NewTopic(PAYMENTS_TOPIC, num_partitions=ORDERS_PARTITIONS, replication_factor=1),
    NewTopic(ORDERS_DLQ_TOPIC, num_partitions=1, replication_factor=1),
]


def main() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    # create_topics is asynchronous: it returns one future per topic.
    for name, future in admin.create_topics(TOPICS).items():
        try:
            future.result()
            print(f"created  {name}")
        except Exception as exc:  # noqa: BLE001
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                print(f"exists   {name}")
            else:
                raise

    metadata = admin.list_topics(timeout=10)
    for name in sorted(t.topic for t in TOPICS):
        print(f"  {name}: {len(metadata.topics[name].partitions)} partition(s)")


if __name__ == "__main__":
    main()
