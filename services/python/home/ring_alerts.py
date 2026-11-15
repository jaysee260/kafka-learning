"""Sends a phone alert when a door or window opens or closes, or a camera
sees motion.

    python -m home.ring_alerts

This file is the whole story. The plumbing is in two helpers you only need
to open if you are curious: kafka_io.py (reading from Redpanda) and
ntfy.py (reaching your phone).
"""

from home.kafka_io import DeviceNames, new_events
from home.ntfy import send_to_phone

EVENT_TOPICS = ["home.ring.contact", "home.ring.motion"]
DEVICES_TOPIC = "home.ring.devices"
GROUP = "ring-alerts"


def what_happened(event):
    """A few words for the phone about one Ring event, or None to stay quiet."""
    kind = event.get("kind")
    state = event.get("state")
    from_camera = event.get("category") == "camera"

    # The first reading for a device is "how things stood", not news.
    if event.get("initial"):
        return None

    if kind == "contact" and state == "ON":
        return "Opened"
    if kind == "contact" and state == "OFF":
        return "Closed"
    if kind == "motion" and state == "ON" and from_camera:
        return "Motion detected"

    return None


def main():
    names = DeviceNames(DEVICES_TOPIC)

    for event, happened_at in new_events(EVENT_TOPICS, GROUP):
        what = what_happened(event)
        if what:
            device = names.of(event.get("device_id"))
            send_to_phone(title=device, message=what, happened_at=happened_at)


if __name__ == "__main__":
    main()
