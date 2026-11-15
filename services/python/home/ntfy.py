"""Reaching your phone, by way of the ntfy server in the hub."""

import base64
import json
import os
import time
import urllib.error
import urllib.request

NTFY_URL = os.getenv("NTFY_URL", "http://localhost:2586")
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "home-alerts")
NTFY_USER = os.getenv("NTFY_USER", "hub-publisher")
NTFY_PASSWORD = os.getenv("NTFY_PASSWORD", "")

# Something that happened longer ago than this is no longer worth a buzz.
MAX_AGE_SECONDS = float(os.getenv("MAX_AGE_SECONDS", "180"))


def send_to_phone(title, message, happened_at):
    """Keeps trying until ntfy takes the alert, or it is too old to matter.

    Kafka can promise we see every event. It cannot promise ntfy is up. If
    we gave up after one failed try, an ntfy restart would swallow alerts
    silently. If we retried forever, we would sit on one old event while
    newer ones queue up, and you would get a burst of stale "Opened" alerts
    when ntfy came back.

    So: retry, but only while the event is still news. The same rule covers
    this service having been down: when it resumes, events older than
    MAX_AGE_SECONDS are dropped instead of sent late.
    """
    wait = 1.0
    while True:
        age = time.time() - happened_at
        if age > MAX_AGE_SECONDS:
            print(f"dropped, {age:.0f}s old: {title}: {message}")
            return
        try:
            _post(title, message)
            print(f"sent: {title}: {message}")
            return
        except (urllib.error.URLError, OSError) as error:
            # Covers both "ntfy is not answering" and "ntfy said no", such
            # as a wrong password.
            print(f"ntfy problem ({error}), trying again in {wait:.0f}s")
            time.sleep(wait)
            wait = min(wait * 2, 15)


def _post(title, message):
    body = json.dumps({"topic": NTFY_TOPIC, "title": title, "message": message}).encode()
    login = base64.b64encode(f"{NTFY_USER}:{NTFY_PASSWORD}".encode()).decode()
    request = urllib.request.Request(NTFY_URL, data=body, method="POST", headers={
        "Content-Type": "application/json",
        "Authorization": f"Basic {login}",
    })
    with urllib.request.urlopen(request, timeout=10) as response:
        response.read()
