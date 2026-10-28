"""阈值规则引擎：持续时长 + 恢复 + 冷却，避免"抖一下就报警"和"报警刷屏"."""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from typing import Callable, Iterable

OPS: dict[str, Callable[[float, float], bool]] = {
    ">": lambda v, t: v > t,
    ">=": lambda v, t: v >= t,
    "<": lambda v, t: v < t,
    "<=": lambda v, t: v <= t,
}


@dataclass(frozen=True)
class Rule:
    """一条阈值规则.

    duration_s: 条件需连续满足多少秒才真正报警（抑制毛刺）
    cooldown_s: 恢复后多久内不再重复报警（抑制抖动刷屏）
    """

    id: str
    metric: str
    op: str
    threshold: float
    device: str = "*"
    duration_s: float = 0.0
    cooldown_s: float = 60.0
    severity: str = "warning"

    def matches(self, device_id: str) -> bool:
        return self.device in ("*", device_id)

    def trigger(self, value: float) -> bool:
        op = OPS.get(self.op)
        if op is None:
            raise ValueError(f"unsupported operator: {self.op!r}")
        return op(value, self.threshold)

    @property
    def message(self) -> str:
        return f"{self.metric} {self.op} {self.threshold:g}"


def parse_rules(raw: str | Iterable[dict]) -> list[Rule]:
    """支持 JSON 字符串或已是 list[dict] 的配置；非法规则直接抛错而不是静默跳过."""
    data = json.loads(raw) if isinstance(raw, str) else list(raw)
    rules: list[Rule] = []
    seen: set[str] = set()
    for item in data:
        rule = Rule(
            id=str(item["id"]),
            metric=str(item["metric"]),
            op=str(item.get("op", ">")),
            threshold=float(item["threshold"]),
            device=str(item.get("device", "*")),
            duration_s=float(item.get("duration_s", 0.0)),
            cooldown_s=float(item.get("cooldown_s", 60.0)),
            severity=str(item.get("severity", "warning")),
        )
        if rule.id in seen:
            raise ValueError(f"duplicate rule id: {rule.id}")
        if rule.op not in OPS:
            raise ValueError(f"rule {rule.id}: unsupported op {rule.op!r}")
        seen.add(rule.id)
        rules.append(rule)
    return rules


@dataclass
class _State:
    """每条 (rule, device, metric) 的运行态."""

    bad_since: int | None = None   # 条件首次成立的毫秒时间戳
    active_alert_id: int | None = None
    last_fire_ts: int = 0


@dataclass(frozen=True)
class AlertEvent:
    kind: str          # "fire" | "resolve"
    rule: Rule
    device_id: str
    value: float
    ts: int
    alert_id: int | None = None


@dataclass
class RuleEngine:
    """纯内存状态机 + 回调；不碰数据库，方便单测."""

    rules: list[Rule]
    on_alert: Callable[[AlertEvent], int | None] | None = None
    _states: dict[tuple[str, str, str], _State] = field(default_factory=dict, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def _state(self, rule: Rule, device_id: str) -> _State:
        key = (rule.id, device_id, rule.metric)
        st = self._states.get(key)
        if st is None:
            st = _State()
            self._states[key] = st
        return st

    def evaluate(self, device_id: str, metric: str, value: float, ts: int) -> list[AlertEvent]:
        """喂一个样本，返回本次产生的事件（可能同时触发多条规则）."""
        events: list[AlertEvent] = []
        with self._lock:
            for rule in self.rules:
                if rule.metric != metric or not rule.matches(device_id):
                    continue
                events.extend(self._evaluate_one(rule, device_id, value, ts))
        return events

    def _evaluate_one(self, rule: Rule, device_id: str, value: float,
                      ts: int) -> list[AlertEvent]:
        st = self._state(rule, device_id)
        out: list[AlertEvent] = []
        hit = rule.trigger(value)

        if st.active_alert_id is not None:
            if not hit:  # 恢复
                alert_id = st.active_alert_id
                st.active_alert_id = None
                st.bad_since = None
                st.last_fire_ts = ts
                event = AlertEvent("resolve", rule, device_id, value, ts, alert_id)
                if self.on_alert is not None:
                    self.on_alert(event)  # 恢复也要走回调，否则库里的告警永远关不掉
                out.append(event)
            return out

        if not hit:
            st.bad_since = None
            return out

        if st.bad_since is None:
            st.bad_since = ts
        if (ts - st.bad_since) < rule.duration_s * 1000:
            return out  # 还没持续够时长，先不报
        if st.last_fire_ts and (ts - st.last_fire_ts) < rule.cooldown_s * 1000:
            return out  # 冷却期内不重复报

        event = AlertEvent("fire", rule, device_id, value, ts)
        alert_id = self.on_alert(event) if self.on_alert else None
        st.active_alert_id = alert_id if alert_id is not None else -1
        st.last_fire_ts = ts
        out.append(AlertEvent("fire", rule, device_id, value, ts, alert_id))
        return out

    def active_count(self) -> int:
        with self._lock:
            return sum(1 for st in self._states.values() if st.active_alert_id is not None)
