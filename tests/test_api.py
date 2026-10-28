"""HTTP 接口测试：入站/查询/错误码，以及静态看板首页."""

from __future__ import annotations

import json
import os
import time
import tempfile
import unittest

from mqttdash.store import Store

try:
    from fastapi.testclient import TestClient

    from mqttdash.api import create_app
    from mqttdash.ingest import Pipeline
    from mqttdash.rules import Rule, RuleEngine

    HAVE_CLIENT = True
except Exception:  # pragma: no cover - 依赖缺失时跳过而不是报错
    HAVE_CLIENT = False


NOW = 1_700_000_000_000


@unittest.skipUnless(HAVE_CLIENT, "需要 fastapi + httpx")
class TestApi(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(os.path.join(self.tmp.name, "a.db"))
        engine = RuleEngine(rules=[Rule("hot", "temp", ">", 30.0)], on_alert=None)
        self.pipeline = Pipeline(self.store, engine)
        self.client = TestClient(create_app(self.store, self.pipeline))

    def tearDown(self) -> None:
        self.client.close()
        self.store.close()
        self.tmp.cleanup()

    def test_healthz(self) -> None:
        r = self.client.get("/healthz")
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json()["ok"])

    def test_ingest_then_query(self) -> None:
        ts = int(time.time() * 1000)
        body = json.dumps({"ts": ts, "metrics": {"temp": 26.0, "hum": 55.0}})
        r = self.client.post("/api/ingest?device=dev-1", content=body,
                             headers={"Content-Type": "application/json"})
        self.assertEqual(r.status_code, 202, r.text)
        self.assertEqual(r.json()["written"], 2)

        devices = self.client.get("/api/devices").json()
        self.assertEqual(devices[0]["device_id"], "dev-1")
        self.assertIn("temp", devices[0]["metrics"])

        series = self.client.get("/api/series?device=dev-1&metric=temp&window=3600&bucket=0").json()
        self.assertEqual(len(series["points"]), 1)
        self.assertAlmostEqual(series["points"][0]["avg"], 26.0)

    def test_ingest_bad_payload_returns_400_and_writes_nothing(self) -> None:
        r = self.client.post("/api/ingest?device=dev-1", content="{bad",
                             headers={"Content-Type": "application/json"})
        self.assertEqual(r.status_code, 400)
        self.assertIn("json", r.json()["detail"].lower())
        self.assertEqual(self.client.get("/api/devices").json(), [])

    def test_ingest_requires_device(self) -> None:
        r = self.client.post("/api/ingest", content="{}")
        self.assertEqual(r.status_code, 422)

    def test_alert_visible_via_api(self) -> None:
        body = json.dumps({"ts": NOW, "metrics": {"temp": 35.0}})
        self.client.post("/api/ingest?device=dev-9", content=body)
        alerts = self.client.get("/api/alerts?only_active=true").json()
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["rule_id"], "hot")
        self.assertEqual(alerts[0]["severity"], "warning")

    def test_stats_and_rules(self) -> None:
        self.client.post("/api/ingest?device=dev-1",
                         content=json.dumps({"ts": NOW, "metrics": {"temp": 20.0}}))
        stats = self.client.get("/api/stats").json()
        self.assertGreaterEqual(stats["samples"], 1)
        self.assertEqual(stats["devices"], 1)
        rules = self.client.get("/api/rules").json()
        self.assertEqual(rules[0]["id"], "hot")

    def test_series_validation(self) -> None:
        self.assertEqual(self.client.get("/api/series?device=d&metric=m&window=5").status_code, 422)
        self.assertEqual(self.client.get("/api/series?metric=m").status_code, 422)

    def test_index_served(self) -> None:
        r = self.client.get("/")
        self.assertEqual(r.status_code, 200)
        self.assertIn("mqtt-dashboard", r.text)

    def test_unknown_route_404(self) -> None:
        self.assertEqual(self.client.get("/api/nope").status_code, 404)


if __name__ == "__main__":
    unittest.main()
