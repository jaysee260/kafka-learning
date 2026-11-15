"""Turns Ring events into phone notifications.

    home.ring.contact --+
    home.ring.motion  --+--> this consumer --HTTP--> ntfy --> your phone
    home.ring.devices --+    (the lookup table)

Run it:  python -m home.ring_alerts

It sends an alert when
  - a contact sensor (door or window) opens or closes
  - a camera sees motion

Three Kafka ideas are at work here, each marked where it happens below:

  1. A consumer group that starts at the END of the topics, because old
     events are not news.
  2. A lookup table built from a compacted topic, to turn a device id into
     a name ("Back Door"). Kafka Streams calls this a stream-table join.
  3. At-least-once delivery to a system outside Kafka, with a rule for
     when to give up.
"""

import base64
import json
import os
import signal
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field

from confluent_kafka import (
    OFFSET_BEGINNING,
    TIMESTAMP_NOT_AVAILABLE,
    Consumer,
    KafkaError,
    TopicPartition,
)

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:19092")
GROUP_ID = os.getenv("GROUP_ID", "ring-alerts")
EVENT_TOPICS = os.getenv("EVENT_TOPICS", "home.ring.contact,home.ring.motion").split(",")
DEVICES_TOPIC = os.getenv("DEVICES_TOPIC", "home.ring.devices")

NTFY_URL = os.getenv("NTFY_URL", "http://localhost:2586")
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "home-alerts")
NTFY_USER = os.getenv("NTFY_USER", "hub-publisher")
NTFY_PASSWORD = os.getenv("NTFY_PASSWORD", "")

# An alert about something that happened longer ago than this is dropped
# instead of sent. See deliver().
MAX_AGE_SECONDS = float(os.getenv("MAX_AGE_SECONDS", "180"))


@dataclass
class Alert:
    title: str
    message: str
    tags: list[str] = field(default_factory=list)  # ntfy shows these as emoji


def alert_for(event: dict, devices: dict[str, dict]) -> Alert | None:
    """Decides whether one event deserves an alert, and what it says.

    This is the whole "business logic" of the service, kept free of Kafka
    and HTTP so it can be tested with plain dictionaries (see
    test_ring_alerts.py). To change what you get alerted about, change this.
    """
    # The bridge marks the first value it ever forwards for a device as
    # initial: "this is how things stood", not "this just happened".
    if event.get("initial"):
        return None

    device_id = str(event.get("device_id", ""))
    device = devices.get(device_id, {})
    name = str(device.get("name") or "").strip() or f"Unknown device {device_id[:8]}"
    kind, state = event.get("kind"), event.get("state")

    if kind == "contact":
        if state == "ON":
            message = "Opened"
        elif state == "OFF":
            message = "Closed"
        else:
            return None
        return Alert(name, message, ["door" if "door" in name.lower() else "window"])

    if kind == "motion":
        # The alarm's motion detector and keypad report motion too. Only
        # cameras are wanted here, and only the start of motion: the OFF
        # that follows a minute later is not worth a buzz.
        if event.get("category") == "camera" and state == "ON":
            return Alert(name, "Motion detected", ["video_camera"])

    return None


class DeviceTable:
    """IDEA 2: an in-memory copy of home.ring.devices, kept up to date.

    That topic is compacted: Kafka keeps at least the latest record for
    each key (the device id). So reading it from the beginning and keeping
    the last value seen per key rebuilds the full table, however long the
    service was away. No database needed.

    It has its own consumer because it reads differently from the event
    consumer: every instance wants the WHOLE topic from offset 0, every
    time it starts. So it picks its partitions itself with assign() instead
    of joining a group with subscribe(), and it never commits an offset.
    """

    def __init__(self) -> None:
        self.devices: dict[str, dict] = {}
        self._consumer = Consumer({
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            # The client insists on a group id, but with assign() and no
            # commits the group is never actually used.
            "group.id": f"{GROUP_ID}-devices",
            "enable.auto.commit": False,
        })

        topic = self._consumer.list_topics(DEVICES_TOPIC, timeout=10).topics[DEVICES_TOPIC]
        if topic.error is not None:
            raise RuntimeError(f"{DEVICES_TOPIC}: {topic.error}")
        partitions = [TopicPartition(DEVICES_TOPIC, p, OFFSET_BEGINNING) for p in topic.partitions]
        self._consumer.assign(partitions)

        # Read until we have caught up with where each partition ends right
        # now. Only then is the table complete enough to answer lookups.
        remaining = {}
        for tp in partitions:
            _low, high = self._consumer.get_watermark_offsets(tp, timeout=10)
            if high > 0:
                remaining[tp.partition] = high
        deadline = time.time() + 30
        while remaining and time.time() < deadline:
            msg = self._consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            self._apply(msg)
            if msg.offset() + 1 >= remaining.get(msg.partition(), 0):
                remaining.pop(msg.partition(), None)

    def refresh(self) -> None:
        """Picks up devices added or renamed since the last call."""
        while True:
            msg = self._consumer.poll(0)
            if msg is None:
                return
            if not msg.error():
                self._apply(msg)

    def _apply(self, msg) -> None:
        if msg.key() is None:
            return
        key = msg.key().decode()
        if msg.value() is None:
            # A record with a key and no value is a "tombstone": the way to
            # delete a key from a compacted topic.
            self.devices.pop(key, None)
            return
        try:
            self.devices[key] = json.loads(msg.value())
        except ValueError:
            pass

    def close(self) -> None:
        self._consumer.close()


def send(alert: Alert) -> None:
    """Publishes one notification to ntfy. Raises if ntfy did not accept it."""
    body = json.dumps({
        "topic": NTFY_TOPIC,
        "title": alert.title,
        "message": alert.message,
        "tags": alert.tags,
    }).encode()
    credentials = base64.b64encode(f"{NTFY_USER}:{NTFY_PASSWORD}".encode()).decode()
    request = urllib.request.Request(NTFY_URL, data=body, method="POST", headers={
        "Content-Type": "application/json",
        "Authorization": f"Basic {credentials}",
    })
    with urllib.request.urlopen(request, timeout=10) as response:
        response.read()


def deliver(alert: Alert, happened_at: float) -> None:
    """IDEA 3: keep trying until ntfy takes it, or it is too old to matter.

    Kafka can promise this service sees every event. It cannot promise ntfy
    is up. If we gave up after one failed try, an ntfy restart would
    swallow alerts silently. If we retried forever, the service would sit
    on one old event while newer ones queue up behind it, and you would get
    a burst of stale "door opened" alerts when ntfy came back.

    So: retry, but only while the event is still news. The same rule covers
    this service having been down: on restart it resumes where it left off
    and events older than MAX_AGE_SECONDS are skipped rather than sent.
    """
    delay = 1.0
    while True:
        age = time.time() - happened_at
        if age > MAX_AGE_SECONDS:
            print(f"dropped, {age:.0f}s old: {alert.title}: {alert.message}")
            return
        try:
            send(alert)
            print(f"sent: {alert.title}: {alert.message}")
            return
        except (urllib.error.URLError, OSError) as error:
            # HTTPError (ntfy answered with an error status, such as a wrong
            # password) is a kind of URLError, so it lands here too.
            print(f"ntfy not reachable ({error}), trying again in {delay:.0f}s")
            time.sleep(delay)
            delay = min(delay * 2, 15)


def main() -> None:
    devices = DeviceTable()
    print(f"loaded {len(devices.devices)} devices from {DEVICES_TOPIC}")

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        # IDEA 1: this only applies the very first time the group runs,
        # when it has no committed offsets yet. "earliest" would replay
        # everything in the topics as a flood of alerts about the past.
        # "latest" means "start with whatever happens next". After that
        # first run the group's own committed offsets decide where it
        # resumes, and this setting is ignored. (One gap: a partition has
        # no committed offset until its first event is handled, so a
        # restart before then starts that partition at the end again.)
        "auto.offset.reset": "latest",
        # At-least-once, same as shop/consumer.py: an offset is only stored
        # (and later committed) after the event has been dealt with.
        "enable.auto.commit": True,
        "enable.auto.offset.store": False,
        "partition.assignment.strategy": "cooperative-sticky",
    })
    consumer.subscribe(EVENT_TOPICS)
    print(f"[{GROUP_ID}] watching {', '.join(EVENT_TOPICS)}, alerting to {NTFY_URL}/{NTFY_TOPIC}")

    # `docker stop` sends SIGTERM. Turn it into a normal loop exit so the
    # consumer can commit its offsets and leave the group cleanly.
    running = True

    def stop(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    try:
        while running:
            devices.refresh()
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(msg.error())

            try:
                event = json.loads(msg.value())
            except (TypeError, ValueError):
                event = None
            alert = alert_for(event, devices.devices) if isinstance(event, dict) else None

            if alert is not None:
                # Every Kafka record carries a timestamp, set by whoever
                # produced it. The bridge produces the moment Ring reports,
                # so this is when the door actually moved.
                timestamp_type, timestamp_ms = msg.timestamp()
                if timestamp_type == TIMESTAMP_NOT_AVAILABLE:
                    happened_at = time.time()
                else:
                    happened_at = timestamp_ms / 1000
                deliver(alert, happened_at)

            consumer.store_offsets(message=msg)
    finally:
        consumer.close()
        devices.close()


if __name__ == "__main__":
    main()
