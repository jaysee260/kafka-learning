"""Stage 4 helper: publish one malformed message to `orders`.

    python -m shop.send_bad_message

Use it to see what a "poison pill" does to the consumers, then build the
dead-letter topic handling described in EXERCISES.md.
"""

from confluent_kafka import Producer

from shop.config import BOOTSTRAP_SERVERS, ORDERS_TOPIC

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
producer.produce(ORDERS_TOPIC, value=b"{this is not valid json")
producer.flush(10)
print(f"sent a malformed message to '{ORDERS_TOPIC}'")
