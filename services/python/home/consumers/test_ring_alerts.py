"""Tests for the alert rules. No broker needed:

    python -m unittest home.consumers.test_ring_alerts
"""

import unittest

from home.kafka_io import display_name
from home.consumers.ring_alerts import what_happened


def event(kind, state, category="alarm", initial=False):
    return {"kind": kind, "state": state, "category": category, "initial": initial}


class WhatHappenedTest(unittest.TestCase):
    def test_door_or_window_opening_and_closing(self):
        self.assertEqual(what_happened(event("contact", "ON")), "Opened")
        self.assertEqual(what_happened(event("contact", "OFF")), "Closed")

    def test_camera_motion_alerts_only_when_it_starts(self):
        self.assertEqual(what_happened(event("motion", "ON", "camera")), "Motion detected")
        self.assertIsNone(what_happened(event("motion", "OFF", "camera")))

    def test_alarm_motion_detector_is_silent(self):
        self.assertIsNone(what_happened(event("motion", "ON", "alarm")))

    def test_baseline_records_are_not_news(self):
        self.assertIsNone(what_happened(event("contact", "ON", initial=True)))
        self.assertIsNone(what_happened(event("motion", "ON", "camera", initial=True)))

    def test_other_kinds_and_odd_states_are_ignored(self):
        self.assertIsNone(what_happened(event("battery", "ON")))
        self.assertIsNone(what_happened(event("contact", "unavailable")))
        self.assertIsNone(what_happened({}))


class DisplayNameTest(unittest.TestCase):
    def test_name_is_trimmed(self):
        self.assertEqual(display_name({"name": "Left window "}, "abc"), "Left window")

    def test_unknown_device_gets_a_recognisable_label(self):
        self.assertEqual(display_name(None, "0123456789abcdef"), "Unknown device 01234567")
        self.assertEqual(display_name({"name": ""}, None), "Unknown device None")


if __name__ == "__main__":
    unittest.main()
