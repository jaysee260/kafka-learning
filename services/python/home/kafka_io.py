"""Reading from Redpanda, for the home services.

Two ways of reading, because the two kinds of topic want different things:

    new_events()   a stream: "tell me what happens from now on"
    DeviceNames    a table:  "give me the current value for every key"
"""

import json
import os
import signal
import time

from confluent_kafka import (
    OFFSET_BEGINNING,
    TIMESTAMP_NOT_AVAILABLE,
    Consumer,
    KafkaError,
    TopicPartition,
)

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:19092")


def new_events(topics, group):
    """Yields (event, happened_at) for every new record on the topics, forever.

    `happened_at` is the record's Kafka timestamp in seconds: every record
    carries one, set by whoever produced it.
    """
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": group,
        # Only matters the first time this group ever runs, when it has no
        # saved position. "latest" means start with whatever happens next;
        # "earliest" would replay the topic's whole history. After that
        # first run the group resumes from its own saved offsets. (One gap:
        # a partition has no saved offset until its first event is handled,
        # so a restart before then starts that partition at the end again.)
        "auto.offset.reset": "latest",
        # At-least-once, same as shop/consumer.py: an offset is saved only
        # after the event has been dealt with.
        "enable.auto.commit": True,
        "enable.auto.offset.store": False,
        "partition.assignment.strategy": "cooperative-sticky",
    })
    consumer.subscribe(list(topics))
    print(f"[{group}] watching {', '.join(topics)}")

    # `docker stop` sends SIGTERM. Turn it into a normal exit from the loop
    # so the consumer saves its offsets and leaves the group cleanly.
    stopping = []
    signal.signal(signal.SIGTERM, lambda *_: stopping.append(True))
    signal.signal(signal.SIGINT, lambda *_: stopping.append(True))

    try:
        while not stopping:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(msg.error())

            event = _parse(msg.value())
            if isinstance(event, dict):
                yield event, _timestamp(msg)

            # A generator pauses at `yield` until the caller asks for the
            # next event, which it only does once it has finished with this
            # one. So reaching this line means: handled, safe to move on.
            consumer.store_offsets(message=msg)
    finally:
        consumer.close()


class DeviceNames:
    """Device id -> name, from a compacted topic.

    A compacted topic keeps at least the latest record for each key. So
    reading it from the beginning and remembering the last value per key
    rebuilds the full table, however long this service was away. Kafka
    Streams calls reading a topic this way a "table".

    It gets its own consumer because it reads differently from
    new_events(): it wants the WHOLE topic from offset 0 every time it
    starts. So it picks its partitions itself with assign() instead of
    joining a group with subscribe(), and it never saves an offset.
    """

    def __init__(self, topic):
        self._devices = {}
        self._consumer = Consumer({
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            # The client insists on a group id, but with assign() and no
            # commits the group is never actually used.
            "group.id": f"{topic}-reader",
            "enable.auto.commit": False,
        })

        info = self._consumer.list_topics(topic, timeout=10).topics[topic]
        if info.error is not None:
            raise RuntimeError(f"{topic}: {info.error}")
        partitions = [TopicPartition(topic, p, OFFSET_BEGINNING) for p in info.partitions]
        self._consumer.assign(partitions)

        # Read until caught up with where each partition ends right now.
        # Only then is the table complete enough to answer lookups.
        ends = {}
        for partition in partitions:
            _start, end = self._consumer.get_watermark_offsets(partition, timeout=10)
            if end > 0:
                ends[partition.partition] = end
        deadline = time.time() + 30
        while ends and time.time() < deadline:
            msg = self._consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            self._remember(msg)
            if msg.offset() + 1 >= ends.get(msg.partition(), 0):
                ends.pop(msg.partition(), None)
        print(f"loaded {len(self._devices)} devices from {topic}")

    def of(self, device_id):
        """The device's name, including any rename since the last call."""
        while (msg := self._consumer.poll(0)) is not None:
            if not msg.error():
                self._remember(msg)
        return display_name(self._devices.get(device_id), device_id)

    def _remember(self, msg):
        if msg.key() is None:
            return
        key = msg.key().decode()
        if msg.value() is None:
            # A key with no value is a "tombstone": how a key is deleted
            # from a compacted topic.
            self._devices.pop(key, None)
        elif isinstance(device := _parse(msg.value()), dict):
            self._devices[key] = device


def display_name(device, device_id):
    name = str((device or {}).get("name") or "").strip()
    return name or f"Unknown device {str(device_id)[:8]}"


def _parse(raw):
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


def _timestamp(msg):
    kind, milliseconds = msg.timestamp()
    return time.time() if kind == TIMESTAMP_NOT_AVAILABLE else milliseconds / 1000
