"""A reusable consume loop for the stage 2 services.

Each service is just a group id plus a function that handles one event:

    run_consumer("inventory", handle_order)

Every distinct group id gets its OWN copy of the stream with its own
offsets. That is what lets inventory, notifications and analytics all read
the same `orders` topic without knowing about each other.
"""

from collections.abc import Callable

from confluent_kafka import Consumer, KafkaError, Message

from shop.config import BOOTSTRAP_SERVERS, ORDERS_TOPIC
from shop.events import deserialize

Handler = Callable[[dict, Message], None]


def run_consumer(group_id: str, handle: Handler, topics: list[str] | None = None) -> None:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        # At-least-once processing:
        # The background committer only commits offsets we have explicitly
        # "stored". We store an offset only AFTER handle() succeeds, so a
        # crash mid-message means it is redelivered, never silently skipped.
        # The trade-off is a message can be processed twice (stage 4).
        "enable.auto.commit": True,
        "enable.auto.offset.store": False,
        # During a rebalance, only the partitions that actually move are
        # revoked, instead of pausing the whole group.
        "partition.assignment.strategy": "cooperative-sticky",
    })

    # Called when the group hands partitions to (or takes them from) this
    # instance. Watch these lines while you start and stop copies (stage 3).
    def on_assign(_consumer, partitions):
        print(f"[{group_id}] assigned partitions: {sorted(p.partition for p in partitions)}")

    def on_revoke(_consumer, partitions):
        print(f"[{group_id}] revoked partitions: {sorted(p.partition for p in partitions)}")

    consumer.subscribe(topics or [ORDERS_TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    print(f"[{group_id}] started (Ctrl+C to stop)")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(msg.error())

            # EXERCISE (stage 4): a malformed message makes deserialize()
            # raise, which crashes the service. Restart it and it crashes
            # again on the same message, because its offset was never
            # stored. Catch the error here and send the raw bytes to the
            # dead-letter topic instead.
            event = deserialize(msg.value())
            handle(event, msg)

            consumer.store_offsets(message=msg)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
