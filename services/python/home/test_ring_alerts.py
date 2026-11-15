"""Tests for the alert rules. No broker needed:

    python -m unittest home.test_ring_alerts
"""

import unittest

from home.ring_alerts import alert_for

DEVICES = {
    "door-1": {"name": "Back Door", "category": "alarm", "model": "Contact Sensor"},
    "window-1": {"name": "Left window ", "category": "alarm", "model": "Contact Sensor"},
    "cam-1": {"name": "Backyard", "category": "camera", "model": "Spotlight Cam Plus"},
    "detector-1": {"name": "Motion Detector", "category": "alarm", "model": "Motion Sensor"},
}


def event(kind, device_id, state, category="alarm", initial=False):
    return {"kind": kind, "device_id": device_id, "state": state,
            "category": category, "initial": initial}


class AlertForTest(unittest.TestCase):
    def test_door_opening_alerts_with_its_name(self):
        alert = alert_for(event("contact", "door-1", "ON"), DEVICES)
        self.assertEqual((alert.title, alert.message, alert.tags), ("Back Door", "Opened", ["door"]))

    def test_door_closing_alerts_too(self):
        alert = alert_for(event("contact", "door-1", "OFF"), DEVICES)
        self.assertEqual(alert.message, "Closed")

    def test_window_name_is_trimmed_and_tagged_as_window(self):
        alert = alert_for(event("contact", "window-1", "ON"), DEVICES)
        self.assertEqual((alert.title, alert.tags), ("Left window", ["window"]))

    def test_baseline_records_are_not_news(self):
        self.assertIsNone(alert_for(event("contact", "door-1", "ON", initial=True), DEVICES))
        self.assertIsNone(alert_for(event("motion", "cam-1", "ON", "camera", initial=True), DEVICES))

    def test_camera_motion_alerts_when_it_starts(self):
        alert = alert_for(event("motion", "cam-1", "ON", "camera"), DEVICES)
        self.assertEqual((alert.title, alert.message), ("Backyard", "Motion detected"))

    def test_camera_motion_ending_is_silent(self):
        self.assertIsNone(alert_for(event("motion", "cam-1", "OFF", "camera"), DEVICES))

    def test_alarm_motion_detector_is_silent(self):
        self.assertIsNone(alert_for(event("motion", "detector-1", "ON", "alarm"), DEVICES))

    def test_unknown_device_still_alerts(self):
        alert = alert_for(event("contact", "0123456789abcdef", "ON"), DEVICES)
        self.assertEqual(alert.title, "Unknown device 01234567")

    def test_other_kinds_and_odd_states_are_ignored(self):
        self.assertIsNone(alert_for(event("battery", "door-1", "ON"), DEVICES))
        self.assertIsNone(alert_for(event("contact", "door-1", "unavailable"), DEVICES))
        self.assertIsNone(alert_for({}, DEVICES))


if __name__ == "__main__":
    unittest.main()
