"""入站校验与管线测试：坏数据必须被拒且不落库，好数据要触发告警."""

from __future__ import annotations

import json
import os
import tempfile
import unittest

from mqttdash.ingest import MAX_METRICS, PayloadError, Pipeline, parse_payload
from mqttdash.rules import Rule, RuleEngine
from mqttdash.store import Store

NOW = 1_700_000_000_000


class TestParsePayload(unittest.TestCase):
    def test_standard_shape(self) -> None:
        body = json.dumps({"ts": NOW, "metrics": {"temp": 25.5, "hum": 60}})
        samples = parse_payload("dev-1", body)
        self.assertEqual(len(samples), 2)
        self.assertEqual({s.metric for s in samples}, {"temp", "hum"})
        self.assertEqual(samples[0].ts, NOW)

    def test_seconds_ts_upgraded_to_millis(self) -> None:
        samples = parse_payload("dev-1", json.dumps({"ts": 1_700_000_000, "metrics": {"t": 1}}))
        self.assertEqual(samples[0].ts, 1_700_000_000_000)

    def test_flat_shape(self) -> None:
        samples = parse_payload("dev-1", json.dumps({"temp": 20, "hum": 50}))
        self.assertEqual(len(samples), 2)

    def test_bare_number_with_hint(self) -> None:
        samples = parse_payload("dev-1", "42.5", ts_hint=NOW)
        self.assertEqual(samples[0].metric, "value")
        self.assertEqual(samples[0].value, 42.5)

    def test_bare_number_without_hint_rejected(self) -> None:
        with self.assertRaises(PayloadError):
            parse_payload("dev-1", "42.5")

    def test_rejections(self) -> None:
        cases = {
            "empty": "",
            "not json": "{oops",
            "array": "[1,2,3]",
            "no metrics": json.dumps({"ts": NOW}),
            "non numeric": json.dumps({"metrics": {"temp": "hot"}}),
            "bool": json.dumps({"metrics": {"temp": True}}),
            "bad ts": json.dumps({"ts": "now", "metrics": {"t": 1}}),
            "bad utf8": b"\xff\xfe\x00",
        }
        for name, body in cases.items():
            with self.subTest(case=name):
                with self.assertRaises(PayloadError):
                    parse_payload("dev-1", body)

    def test_nan_rejected(self) -> None:
        with self.assertRaises(PayloadError):
            parse_payload("dev-1", '{"metrics": {"temp": NaN}}')

    def test_too_many_metrics(self) -> None:
        body = json.dumps({"metrics": {f"m{i}": 1 for i in range(MAX_METRICS + 1)}})
        with self.assertRaises(PayloadError):
            parse_payload("dev-1", body)

    def test_missing_device(self) -> None:
        with self.assertRaises(PayloadError):
            parse_payload("", json.dumps({"metrics": {"t": 1}}))


class TestPipeline(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(os.path.join(self.tmp.name, "p.db"))

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def test_ingest_writes_and_counts(self) -> None:
        pipe = Pipeline(self.store)
        written = pipe.ingest_raw("dev-1", json.dumps({"ts": NOW, "metrics": {"temp": 1.0}}))
        self.assertEqual(written, 1)
        self.assertEqual(pipe.stats.accepted, 1)
        self.assertEqual(self.store.stats(now_ms=NOW)["samples"], 1)

    def test_bad_payload_does_not_write(self) -> None:
        pipe = Pipeline(self.store)
        with self.assertRaises(PayloadError):
            pipe.ingest_raw("dev-1", "{bad json")
        self.assertEqual(pipe.stats.rejected, 1)
        self.assertEqual(self.store.stats(now_ms=NOW)["samples"], 0)
        self.assertIsNotNone(pipe.stats.last_error)

    def test_alert_written_to_db(self) -> None:
        engine = RuleEngine(rules=[Rule("hot", "temp", ">", 30.0)], on_alert=None)
        pipe = Pipeline(self.store, engine)
        pipe.ingest_raw("dev-1", json.dumps({"ts": NOW, "metrics": {"temp": 31.0}}))
        active = self.store.alerts(only_active=True)
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0].rule_id, "hot")
        self.assertEqual(active[0].device_id, "dev-1")

    def test_alert_resolved_on_recovery(self) -> None:
        engine = RuleEngine(rules=[Rule("hot", "temp", ">", 30.0)], on_alert=None)
        pipe = Pipeline(self.store, engine)
        pipe.ingest_raw("dev-1", json.dumps({"ts": NOW, "metrics": {"temp": 31.0}}))
        pipe.ingest_raw("dev-1", json.dumps({"ts": NOW + 5_000, "metrics": {"temp": 20.0}}))
        self.assertEqual(self.store.alerts(only_active=True), [])
        events = pipe.recent_events()
        self.assertEqual([e["kind"] for e in events], ["resolve", "fire"])

    def test_events_ring_buffer_capped(self) -> None:
        engine = RuleEngine(rules=[Rule("hot", "temp", ">", 30.0, cooldown_s=0)], on_alert=None)
        pipe = Pipeline(self.store, engine)
        for i in range(260):
            ts = NOW + i * 100
            pipe.ingest_raw("dev-1", json.dumps({"ts": ts, "metrics": {"temp": 31.0 if i % 2 == 0 else 20.0}}))
        self.assertLessEqual(len(pipe.recent_events(limit=1000)), 200)


if __name__ == "__main__":
    unittest.main()
