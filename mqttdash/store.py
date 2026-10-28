"""SQLite 存储层：写入走批量+WAL，查询走 SQL 内降采样，避免把百万行拉进内存."""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Iterable, Sequence

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS telemetry (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT    NOT NULL,
    metric    TEXT    NOT NULL,
    ts        INTEGER NOT NULL,          -- 毫秒时间戳（采集端时间）
    value     REAL    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_telemetry_dev_metric_ts
    ON telemetry (device_id, metric, ts);
CREATE INDEX IF NOT EXISTS idx_telemetry_ts ON telemetry (ts);

CREATE TABLE IF NOT EXISTS alerts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id     TEXT    NOT NULL,
    device_id   TEXT    NOT NULL,
    metric      TEXT    NOT NULL,
    severity    TEXT    NOT NULL,
    value       REAL    NOT NULL,
    threshold   REAL    NOT NULL,
    started_ts  INTEGER NOT NULL,
    resolved_ts INTEGER,
    message     TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alerts_device_ts ON alerts (device_id, started_ts);
"""


@dataclass(frozen=True)
class Sample:
    device_id: str
    metric: str
    ts: int
    value: float


@dataclass(frozen=True)
class Bucket:
    ts: int
    avg: float
    min: float
    max: float
    count: int


@dataclass(frozen=True)
class DeviceRow:
    device_id: str
    metrics: int
    samples: int
    first_ts: int
    last_ts: int


@dataclass(frozen=True)
class AlertRow:
    id: int
    rule_id: str
    device_id: str
    metric: str
    severity: str
    value: float
    threshold: float
    started_ts: int
    resolved_ts: int | None
    message: str


class Store:
    """单连接 + 锁：写入方是 MQTT 线程/HTTP 线程，SQLite 的连接复用比开池更省事."""

    def __init__(self, path: str = "mqttdash.db") -> None:
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---- 写入 ----

    def insert_many(self, samples: Sequence[Sample]) -> int:
        if not samples:
            return 0
        rows = [(s.device_id, s.metric, s.ts, s.value) for s in samples]
        with self._lock:
            self._conn.executemany(
                "INSERT INTO telemetry (device_id, metric, ts, value) VALUES (?, ?, ?, ?)", rows
            )
            self._conn.commit()
        return len(rows)

    def insert_alert(self, alert: AlertRow) -> int:
        with self._lock:
            cur = self._conn.execute(
                """INSERT INTO alerts
                   (rule_id, device_id, metric, severity, value, threshold, started_ts, message)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (alert.rule_id, alert.device_id, alert.metric, alert.severity,
                 alert.value, alert.threshold, alert.started_ts, alert.message),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def resolve_alert(self, alert_id: int, resolved_ts: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE alerts SET resolved_ts = ? WHERE id = ? AND resolved_ts IS NULL",
                (resolved_ts, alert_id),
            )
            self._conn.commit()

    # ---- 查询 ----

    def series(self, device_id: str, metric: str, window_s: int = 900,
               bucket_s: int = 30, now_ms: int | None = None) -> list[Bucket]:
        """按时间桶聚合。bucket_s<=0 时返回原始点（用于短窗口看波形）."""
        now = now_ms if now_ms is not None else int(time.time() * 1000)
        since = now - window_s * 1000
        with self._lock:
            if bucket_s <= 0:
                rows = self._conn.execute(
                    """SELECT ts, value AS avg, value AS min, value AS max, 1 AS count
                       FROM telemetry WHERE device_id=? AND metric=? AND ts>=?
                       ORDER BY ts""",
                    (device_id, metric, since),
                ).fetchall()
            else:
                bucket_ms = bucket_s * 1000
                # 别名必须避开 ts：SQLite 的 GROUP BY 优先解析表列名而不是别名，
                # 写成 `GROUP BY ts` 会按原始行分组，聚合等于失效（这个坑由测试抓出）。
                rows = self._conn.execute(
                    """SELECT (ts / ?) * ? AS bucket_ts, AVG(value) AS avg, MIN(value) AS min,
                              MAX(value) AS max, COUNT(*) AS count
                       FROM telemetry WHERE device_id=? AND metric=? AND ts>=?
                       GROUP BY bucket_ts ORDER BY bucket_ts""",
                    (bucket_ms, bucket_ms, device_id, metric, since),
                ).fetchall()
                return [Bucket(int(r["bucket_ts"]), float(r["avg"]), float(r["min"]),
                               float(r["max"]), int(r["count"])) for r in rows]
        return [Bucket(int(r["ts"]), float(r["avg"]), float(r["min"]),
                       float(r["max"]), int(r["count"])) for r in rows]

    def devices(self) -> list[DeviceRow]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT device_id, COUNT(DISTINCT metric) AS metrics, COUNT(*) AS samples,
                          MIN(ts) AS first_ts, MAX(ts) AS last_ts
                   FROM telemetry GROUP BY device_id ORDER BY last_ts DESC"""
            ).fetchall()
        return [DeviceRow(r["device_id"], int(r["metrics"]), int(r["samples"]),
                          int(r["first_ts"]), int(r["last_ts"])) for r in rows]

    def metrics(self, device_id: str) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT metric FROM telemetry WHERE device_id=? ORDER BY metric",
                (device_id,),
            ).fetchall()
        return [r["metric"] for r in rows]

    def alerts(self, limit: int = 50, only_active: bool = False) -> list[AlertRow]:
        sql = "SELECT * FROM alerts"
        if only_active:
            sql += " WHERE resolved_ts IS NULL"
        sql += " ORDER BY started_ts DESC LIMIT ?"
        with self._lock:
            rows = self._conn.execute(sql, (limit,)).fetchall()
        return [
            AlertRow(r["id"], r["rule_id"], r["device_id"], r["metric"], r["severity"],
                     float(r["value"]), float(r["threshold"]), int(r["started_ts"]),
                     None if r["resolved_ts"] is None else int(r["resolved_ts"]), r["message"])
            for r in rows
        ]

    def stats(self, now_ms: int | None = None) -> dict:
        now = now_ms if now_ms is not None else int(time.time() * 1000)
        with self._lock:
            total = self._conn.execute("SELECT COUNT(*) AS c FROM telemetry").fetchone()["c"]
            last = self._conn.execute("SELECT MAX(ts) AS t FROM telemetry").fetchone()["t"]
            devices = self._conn.execute(
                "SELECT COUNT(DISTINCT device_id) AS c FROM telemetry"
            ).fetchone()["c"]
            minute = self._conn.execute(
                "SELECT COUNT(*) AS c FROM telemetry WHERE ts >= ?", (now - 60_000,)
            ).fetchone()["c"]
            active = self._conn.execute(
                "SELECT COUNT(*) AS c FROM alerts WHERE resolved_ts IS NULL"
            ).fetchone()["c"]
        return {
            "samples": int(total),
            "devices": int(devices),
            "samples_per_min": int(minute),
            "active_alerts": int(active),
            "last_ts": None if last is None else int(last),
            "now_ts": now,
        }

    def prune(self, keep_s: int, now_ms: int | None = None) -> int:
        """保留最近 keep_s 秒的数据，返回删除行数（长时间运行的必选项）."""
        now = now_ms if now_ms is not None else int(time.time() * 1000)
        with self._lock:
            cur = self._conn.execute("DELETE FROM telemetry WHERE ts < ?", (now - keep_s * 1000,))
            self._conn.commit()
            return cur.rowcount


def batch_insert(store: Store, samples: Iterable[Sample], max_batch: int = 500) -> int:
    """把任意长度的样本流按批落库，减少 fsync 次数."""
    buf: list[Sample] = []
    written = 0
    for s in samples:
        buf.append(s)
        if len(buf) >= max_batch:
            written += store.insert_many(buf)
            buf.clear()
    written += store.insert_many(buf)
    return written
