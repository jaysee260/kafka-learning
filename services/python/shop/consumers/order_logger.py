"""Stage 1: the simplest possible consumer. Prints every order.

    python -m shop.consumers.order_logger

The poll loop is written out in full here so you can see every step.
The stage 2 services reuse the same loop via shop/consumer.py.
"""

from confluent_kafka import Consumer, KafkaError

from shop.config import BOOTSTRAP_SERVERS, ORDERS_TOPIC
from shop.events import cents, deserialize


def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        # The consumer group. Kafka stores how far each group has read
        # (its committed offsets) so a restart picks up where it left off.
        "group.id": "order-logger",
        # Only used when the group has NO committed offset yet (first run):
        # "earliest" = start from the oldest retained message,
        # "latest"   = only see messages produced from now on.
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([ORDERS_TOPIC])
    print(f"order-logger reading '{ORDERS_TOPIC}' (Ctrl+C to stop)")

    try:
        while True:
            # Wait up to 1s for the next message. None means nothing arrived.
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                # Reaching the end of a partition is informational, not fatal.
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(msg.error())

            order = deserialize(msg.value())
            print(
                f"[p{msg.partition()} @{msg.offset()}] "
                f"{order['customer_id']:<6} ordered {len(order['items'])} item(s), "
                f"total {cents(order['total_cents'])}"
            )
            # Offsets are committed automatically in the background every
            # few seconds (enable.auto.commit defaults to true).
    except KeyboardInterrupt:
        pass
    finally:
        # close() commits final offsets and tells the group we're leaving,
        # so partitions are handed to other members right away.
        consumer.close()


if __name__ == "__main__":
    main()
