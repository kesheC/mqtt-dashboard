"""存储层测试：SQL 降采样、设备汇总、告警写入/恢复、清理."""

from __future__ import annotations

import os
import tempfile
import unittest

from mqttdash.store import AlertRow, Sample, Store, batch_insert

NOW = 1_700_000_000_000


def sample(dev: str, metric: str, ts: int, value: float) -> Sample:
    return Sample(dev, metric, ts, value)


class StoreCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(os.path.join(self.tmp.name, "t.db"))

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()


class TestSeries(StoreCase):
    def test_bucketed_average(self) -> None:
        # 桶对齐绝对时间（epoch 的整数倍），这样刷新页面时柱子不会左右跳动
        aligned = NOW - 60_000
        aligned -= aligned % 30_000  # 落到 30s 的整数倍
        self.store.insert_many([
            sample("dev-1", "temp", aligned, 1.0),
            sample("dev-1", "temp", aligned + 5_000, 2.0),
            sample("dev-1", "temp", aligned + 35_000, 3.0),
            sample("dev-1", "temp", aligned + 40_000, 4.0),
        ])
        buckets = self.store.series("dev-1", "temp", window_s=600, bucket_s=30, now_ms=NOW)
        self.assertEqual(len(buckets), 2)
        self.assertAlmostEqual(buckets[0].avg, 1.5, places=6)
        self.assertAlmostEqual(buckets[0].min, 1.0, places=6)
        self.assertAlmostEqual(buckets[0].max, 2.0, places=6)
        self.assertEqual(buckets[0].count, 2)
        self.assertEqual(buckets[0].ts % 30_000, 0)  # 桶起点必是整数倍
        self.assertAlmostEqual(buckets[1].avg, 3.5, places=6)

    def test_raw_points_when_bucket_zero(self) -> None:
        base = NOW - 10_000
        self.store.insert_many([sample("dev-1", "vib", base + i * 1000, float(i)) for i in range(4)])
        raw = self.store.series("dev-1", "vib", window_s=60, bucket_s=0, now_ms=NOW)
        self.assertEqual([r.avg for r in raw], [0.0, 1.0, 2.0, 3.0])

    def test_window_excludes_old_rows(self) -> None:
        self.store.insert_many([
            sample("dev-1", "temp", NOW - 3_600_000, 99.0),  # 1 小时前，窗口外
            sample("dev-1", "temp", NOW - 5_000, 20.0),
        ])
        buckets = self.store.series("dev-1", "temp", window_s=600, bucket_s=30, now_ms=NOW)
        self.assertEqual(len(buckets), 1)
        self.assertAlmostEqual(buckets[0].avg, 20.0)

    def test_other_metric_ignored(self) -> None:
        self.store.insert_many([
            sample("dev-1", "temp", NOW - 1_000, 20.0),
            sample("dev-1", "hum", NOW - 1_000, 55.0),
        ])
        self.assertEqual(len(self.store.series("dev-1", "temp", now_ms=NOW)), 1)


class TestDevices(StoreCase):
    def test_aggregates(self) -> None:
        self.store.insert_many([
            sample("dev-1", "temp", NOW - 3_000, 1.0),
            sample("dev-1", "hum", NOW - 2_000, 2.0),
            sample("dev-2", "temp", NOW - 1_000, 3.0),
        ])
        rows = {r.device_id: r for r in self.store.devices()}
        self.assertEqual(set(rows), {"dev-1", "dev-2"})
        self.assertEqual(rows["dev-1"].metrics, 2)
        self.assertEqual(rows["dev-1"].samples, 2)
        self.assertEqual(rows["dev-1"].first_ts, NOW - 3_000)
        self.assertEqual(rows["dev-1"].last_ts, NOW - 2_000)
        self.assertEqual(self.store.metrics("dev-1"), ["hum", "temp"])
        # 最近活跃的设备排前面
        self.assertEqual(self.store.devices()[0].device_id, "dev-2")


class TestAlerts(StoreCase):
    def test_insert_and_resolve(self) -> None:
        alert_id = self.store.insert_alert(AlertRow(
            id=0, rule_id="temp-high", device_id="dev-1", metric="temp",
            severity="warning", value=31.2, threshold=30.0,
            started_ts=NOW - 5_000, resolved_ts=None, message="dev-1 temp > 30",
        ))
        self.assertGreater(alert_id, 0)
        rows = self.store.alerts(only_active=True)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].rule_id, "temp-high")
        self.assertIsNone(rows[0].resolved_ts)

        self.store.resolve_alert(alert_id, NOW)
        self.assertEqual(len(self.store.alerts(only_active=True)), 0)
        all_rows = self.store.alerts()
        self.assertEqual(all_rows[0].resolved_ts, NOW)

    def test_resolve_is_idempotent(self) -> None:
        alert_id = self.store.insert_alert(AlertRow(
            id=0, rule_id="r", device_id="d", metric="m", severity="info",
            value=1.0, threshold=0.0, started_ts=NOW - 1, resolved_ts=None, message="x",
        ))
        self.store.resolve_alert(alert_id, NOW)
        self.store.resolve_alert(alert_id, NOW + 5_000)  # 二次调用不应覆盖首次恢复时间
        self.assertEqual(self.store.alerts()[0].resolved_ts, NOW)


class TestStatsAndPrune(StoreCase):
    def test_stats(self) -> None:
        self.store.insert_many([
            sample("dev-1", "temp", NOW - 30_000, 1.0),
            sample("dev-1", "temp", NOW - 20_000, 2.0),
            sample("dev-2", "hum", NOW - 90_000, 3.0),
        ])
        st = self.store.stats(now_ms=NOW)
        self.assertEqual(st["samples"], 3)
        self.assertEqual(st["devices"], 2)
        self.assertEqual(st["samples_per_min"], 2)  # 只有两条落在最近 60s
        self.assertEqual(st["last_ts"], NOW - 20_000)

    def test_prune(self) -> None:
        self.store.insert_many([
            sample("dev-1", "temp", NOW - 10 * 86_400_000, 1.0),
            sample("dev-1", "temp", NOW - 1_000, 2.0),
        ])
        removed = self.store.prune(keep_s=3600, now_ms=NOW)
        self.assertEqual(removed, 1)
        self.assertEqual(self.store.stats(now_ms=NOW)["samples"], 1)

    def test_batch_insert(self) -> None:
        samples = [sample("dev-1", "temp", NOW - i * 1000, float(i)) for i in range(25)]
        self.assertEqual(batch_insert(self.store, samples, max_batch=10), 25)
        self.assertEqual(self.store.stats(now_ms=NOW)["samples"], 25)


if __name__ == "__main__":
    unittest.main()
