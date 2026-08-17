import unittest

from event_api_response import EventApiResponseError, normalize_event_api_response


class EventApiResponseTests(unittest.TestCase):
    def test_success_responses_accept_missing_null_and_zero_status(self):
        for payload in (
            {"events": {}},
            {"status": None, "events": {}},
            {"status": 0, "events": {}},
            {"status": "0", "events": {}},
        ):
            with self.subTest(payload=payload):
                self.assertEqual(normalize_event_api_response(payload), payload)

    def test_empty_event_array_is_normalized_without_mutating_input(self):
        payload = {"status": 0, "events": []}

        normalized = normalize_event_api_response(payload)

        self.assertEqual(normalized, {"status": 0, "events": {}})
        self.assertEqual(payload["events"], [])
        self.assertIsNot(normalized, payload)

    def test_nonzero_and_boolean_statuses_are_rejected(self):
        for status in (1, -1, "1", True, False):
            with self.subTest(status=status):
                with self.assertRaises(EventApiResponseError) as raised:
                    normalize_event_api_response({"status": status, "events": {}})
                self.assertEqual(raised.exception.reason, "status")

    def test_invalid_top_level_and_events_shapes_are_rejected(self):
        with self.assertRaises(EventApiResponseError) as raised:
            normalize_event_api_response([])
        self.assertEqual(raised.exception.reason, "object")

        for events in (None, True, "not-an-object", [{"code": "G"}]):
            with self.subTest(events=events):
                with self.assertRaises(EventApiResponseError) as raised:
                    normalize_event_api_response({"status": 0, "events": events})
                self.assertEqual(raised.exception.reason, "events")


if __name__ == "__main__":
    unittest.main()
