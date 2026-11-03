"""Stage 1: the storefront. Emits OrderPlaced events to the `orders` topic.

    python -m shop.producer                 # one order per second, forever
    python -m shop.producer --rate 10       # ten per second
    python -m shop.producer --count 20      # send 20 then exit
"""

import argparse
import time

from confluent_kafka import KafkaError, Message, Producer

from shop.config import BOOTSTRAP_SERVERS, ORDERS_TOPIC
from shop.events import cents, make_order_placed, serialize


def on_delivery(err: KafkaError | None, msg: Message) -> None:
    """Called once the broker has acknowledged (or rejected) a message.

    produce() only puts the message in a local buffer; this callback is
    how you learn where it actually landed. Note the partition and offset:
    an offset is the message's position within its partition.
    """
    if err is not None:
        print(f"  !! delivery failed: {err}")
        return
    key = msg.key().decode() if msg.key() else None
    print(f"  -> {msg.topic()} partition={msg.partition()} offset={msg.offset()} key={key}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rate", type=float, default=1.0, help="orders per second (default 1)")
    parser.add_argument("--count", type=int, default=0, help="stop after N orders (default: run forever)")
    args = parser.parse_args()

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        # Wait for the broker to fully persist each message before acking.
        # With one broker this is the same as acks=1, but it is the safe
        # default to build habits around.
        "acks": "all",
        # Retries can't create duplicates when idempotence is on.
        "enable.idempotence": True,
    })

    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            order = make_order_placed()
            print(f"OrderPlaced {order['order_id'][:8]} {order['customer_id']:<6} {cents(order['total_cents'])}")

            producer.produce(
                ORDERS_TOPIC,
                value=serialize(order),
                # EXERCISE (stage 3): messages with no key are spread across
                # partitions. Set key=order["customer_id"] and watch every
                # order for one customer land in the same partition.
                key=None,
                on_delivery=on_delivery,
            )
            # poll() gives the client a chance to run delivery callbacks.
            producer.poll(0)
            sent += 1
            time.sleep(1 / args.rate)
    except KeyboardInterrupt:
        pass
    finally:
        # flush() blocks until every buffered message is delivered. Skip it
        # and the last few orders may never leave your laptop.
        print("flushing...")
        producer.flush(10)


if __name__ == "__main__":
    main()
