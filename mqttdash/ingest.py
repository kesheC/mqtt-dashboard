"""入站管线：校验 payload → 拆成 Sample → 落库 → 过规则引擎 → 推送告警."""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass

from .rules import AlertEvent, RuleEngine
from .store import AlertRow, Sample, Store

MAX_METRICS = 64


class PayloadError(ValueError):
    """入站数据不合法。调用方应回 400，且不要把脏数据写进库."""


@dataclass
class IngestStats:
    accepted: int = 0
    rejected: int = 0
    last_error: str | None = None

    def as_dict(self) -> dict:
        return {"accepted": self.accepted, "rejected": self.rejected,
                "last_error": self.last_error}


def parse_payload(device_id: str, body: bytes | str, ts_hint: int | None = None) -> list[Sample]:
    """约定 payload: {"ts": 1690000000000, "metrics": {"temp": 25.3, "hum": 61}}

    也接受裸 {"temp": 25.3}（无 metrics 包装）与单值 25.3（此时必须有 ts_hint）。
    任何非数值/非字典结构都抛 PayloadError —— 宁可拒收也不给库里塞脏数据。
    """
    if not device_id or not isinstance(device_id, str):
        raise PayloadError("device id required")

    if isinstance(body, bytes):
        try:
            body = body.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise PayloadError(f"payload is not utf-8: {exc}") from exc

    text = body.strip()
    if not text:
        raise PayloadError("empty payload")

    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise PayloadError(f"invalid json: {exc.msg}") from exc

    ts = ts_hint if ts_hint is not None else int(time.time() * 1000)

    if isinstance(data, (int, float)):
        if ts_hint is None:
            raise PayloadError("bare numeric payload requires explicit ts")
        return [Sample(device_id, "value", int(ts), float(data))]

    if not isinstance(data, dict):
        raise PayloadError("payload must be an object")

    if "ts" in data:
        raw_ts = data["ts"]
        if isinstance(raw_ts, bool) or not isinstance(raw_ts, (int, float)):
            raise PayloadError("ts must be a number (epoch millis)")
        ts = int(raw_ts)
        # 秒级时间戳自动升级为毫秒，避免前端排序错乱
        if ts < 10_000_000_000:
            ts *= 1000

    metrics = data.get("metrics", data)
    if not isinstance(metrics, dict):
        raise PayloadError("metrics must be an object")
    if "ts" in metrics:
        metrics = {k: v for k, v in metrics.items() if k != "ts"}
    if not metrics:
        raise PayloadError("no metrics in payload")
    if len(metrics) > MAX_METRICS:
        raise PayloadError(f"too many metrics ({len(metrics)} > {MAX_METRICS})")

    out: list[Sample] = []
    for name, value in metrics.items():
        if not isinstance(name, str) or not name:
            raise PayloadError("metric name must be a non-empty string")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise PayloadError(f"metric {name!r} must be numeric")
        if value != value or value in (float("inf"), float("-inf")):
            raise PayloadError(f"metric {name!r} must be finite")
        out.append(Sample(device_id, name, ts, float(value)))
    return out


class Pipeline:
    """把 store / rule engine / 统计串起来，HTTP 与 MQTT 两个入口共用."""

    def __init__(self, store: Store, engine: RuleEngine | None = None) -> None:
        self.store = store
        self.engine = engine or RuleEngine(rules=[])
        self.stats = IngestStats()
        self._lock = threading.Lock()
        self._events: list[AlertEvent] = []
        self.engine.on_alert = self._on_alert

    # ---- 规则引擎回调 ----

    def _on_alert(self, event: AlertEvent) -> int | None:
        if event.kind == "fire":
            alert_id = self.store.insert_alert(
                AlertRow(
                    id=0,
                    rule_id=event.rule.id,
                    device_id=event.device_id,
                    metric=event.rule.metric,
                    severity=event.rule.severity,
                    value=event.value,
                    threshold=event.rule.threshold,
                    started_ts=event.ts,
                    resolved_ts=None,
                    message=f"{event.device_id} {event.rule.message} (当前 {event.value:g})",
                )
            )
            with self._lock:
                self._events.append(event)
                del self._events[:-200]
            return alert_id
        # resolve：更新库里的 resolved_ts
        if event.alert_id and event.alert_id > 0:
            self.store.resolve_alert(event.alert_id, event.ts)
        with self._lock:
            self._events.append(event)
            del self._events[:-200]
        return None

    # ---- 入口 ----

    def ingest_raw(self, device_id: str, body: bytes | str,
                   ts_hint: int | None = None) -> int:
        try:
            samples = parse_payload(device_id, body, ts_hint)
        except PayloadError as exc:
            with self._lock:
                self.stats.rejected += 1
                self.stats.last_error = str(exc)
            raise
        return self.ingest_samples(samples)

    def ingest_samples(self, samples: list[Sample]) -> int:
        written = self.store.insert_many(samples)
        for s in samples:
            self.engine.evaluate(s.device_id, s.metric, s.value, s.ts)
        with self._lock:
            self.stats.accepted += written
        return written

    def recent_events(self, limit: int = 20) -> list[dict]:
        with self._lock:
            events = self._events[-limit:]
        return [
            {"kind": e.kind, "rule_id": e.rule.id, "device": e.device_id,
             "metric": e.rule.metric, "value": e.value, "ts": e.ts, "severity": e.rule.severity}
            for e in reversed(events)
        ]
