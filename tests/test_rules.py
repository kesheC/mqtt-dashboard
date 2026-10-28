"""规则引擎测试：持续时长、恢复、冷却、通配设备、配置校验."""

from __future__ import annotations

import unittest

from mqttdash.rules import AlertEvent, Rule, RuleEngine, parse_rules

NOW = 1_700_000_000_000


def engine(rules: list[Rule], sink: list[AlertEvent] | None = None) -> RuleEngine:
    sink = sink if sink is not None else []
    eng = RuleEngine(rules=rules, on_alert=sink.append and (lambda e: len(sink) or 1))
    return eng


class TestBasicTrigger(unittest.TestCase):
    def test_threshold_crossed(self) -> None:
        sink: list[AlertEvent] = []
        eng = RuleEngine(rules=[Rule("r", "temp", ">", 30.0)], on_alert=lambda e: len(sink) or 1)
        events = eng.evaluate("dev-1", "temp", 31.0, NOW)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].kind, "fire")
        self.assertEqual(eng.active_count(), 1)

    def test_no_trigger_below_threshold(self) -> None:
        eng = RuleEngine(rules=[Rule("r", "temp", ">", 30.0)], on_alert=lambda e: 1)
        self.assertEqual(eng.evaluate("dev-1", "temp", 29.9, NOW), [])
        self.assertEqual(eng.active_count(), 0)

    def test_operators(self) -> None:
        for op, value, expected in [("<", 9.0, True), ("<=", 10.0, True),
                                    (">=", 10.0, True), (">", 10.0, False)]:
            rule = Rule("r", "m", op, 10.0)
            self.assertEqual(rule.trigger(value), expected, msg=f"{op} {value}")

    def test_other_metric_or_device_ignored(self) -> None:
        eng = RuleEngine(rules=[Rule("r", "temp", ">", 30.0, device="dev-1")],
                         on_alert=lambda e: 1)
        self.assertEqual(eng.evaluate("dev-2", "temp", 40.0, NOW), [])
        self.assertEqual(eng.evaluate("dev-1", "hum", 40.0, NOW), [])

    def test_wildcard_device(self) -> None:
        eng = RuleEngine(rules=[Rule("r", "temp", ">", 30.0)], on_alert=lambda e: 1)
        self.assertEqual(len(eng.evaluate("any-device", "temp", 40.0, NOW)), 1)


class TestDuration(unittest.TestCase):
    def test_needs_sustained_duration(self) -> None:
        eng = RuleEngine(rules=[Rule("r", "temp", ">", 30.0, duration_s=5)],
                         on_alert=lambda e: 1)
        self.assertEqual(eng.evaluate("dev-1", "temp", 31.0, NOW), [])
        self.assertEqual(eng.evaluate("dev-1", "temp", 31.0, NOW + 3_000), [])
        events = eng.evaluate("dev-1", "temp", 31.0, NOW + 5_000)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].kind, "fire")

    def test_spike_resets_timer(self) -> None:
        """抖一下就回落，不能算"持续满足"."""
        eng = RuleEngine(rules=[Rule("r", "temp", ">", 30.0, duration_s=5)],
                         on_alert=lambda e: 1)
        eng.evaluate("dev-1", "temp", 31.0, NOW)
        eng.evaluate("dev-1", "temp", 29.0, NOW + 1_000)  # 回落，计时清零
        self.assertEqual(eng.evaluate("dev-1", "temp", 31.0, NOW + 2_000), [])
        self.assertEqual(eng.evaluate("dev-1", "temp", 31.0, NOW + 6_000), [])
        self.assertEqual(len(eng.evaluate("dev-1", "temp", 31.0, NOW + 7_000)), 1)


class TestResolveAndCooldown(unittest.TestCase):
    def test_resolve_after_recovery(self) -> None:
        eng = RuleEngine(rules=[Rule("r", "temp", ">", 30.0)], on_alert=lambda e: 42)
        fire = eng.evaluate("dev-1", "temp", 31.0, NOW)[0]
        self.assertEqual(fire.alert_id, 42)
        resolve = eng.evaluate("dev-1", "temp", 25.0, NOW + 1_000)[0]
        self.assertEqual(resolve.kind, "resolve")
        self.assertEqual(resolve.alert_id, 42)
        self.assertEqual(eng.active_count(), 0)

    def test_no_duplicate_fire_while_active(self) -> None:
        eng = RuleEngine(rules=[Rule("r", "temp", ">", 30.0)], on_alert=lambda e: 1)
        eng.evaluate("dev-1", "temp", 31.0, NOW)
        self.assertEqual(eng.evaluate("dev-1", "temp", 32.0, NOW + 1_000), [])
        self.assertEqual(eng.evaluate("dev-1", "temp", 33.0, NOW + 2_000), [])

    def test_cooldown_blocks_refire(self) -> None:
        eng = RuleEngine(rules=[Rule("r", "temp", ">", 30.0, cooldown_s=60)],
                         on_alert=lambda e: 1)
        eng.evaluate("dev-1", "temp", 31.0, NOW)
        eng.evaluate("dev-1", "temp", 20.0, NOW + 10_000)   # 恢复
        self.assertEqual(eng.evaluate("dev-1", "temp", 31.0, NOW + 20_000), [])  # 冷却期内
        self.assertEqual(len(eng.evaluate("dev-1", "temp", 31.0, NOW + 75_000)), 1)

    def test_two_devices_independent(self) -> None:
        eng = RuleEngine(rules=[Rule("r", "temp", ">", 30.0)], on_alert=lambda e: 1)
        self.assertEqual(len(eng.evaluate("dev-1", "temp", 31.0, NOW)), 1)
        self.assertEqual(len(eng.evaluate("dev-2", "temp", 31.0, NOW)), 1)
        self.assertEqual(eng.active_count(), 2)


class TestConfigParsing(unittest.TestCase):
    def test_parse_json_string(self) -> None:
        rules = parse_rules('[{"id":"a","metric":"temp","op":">","threshold":30}]')
        self.assertEqual(len(rules), 1)
        self.assertEqual(rules[0].severity, "warning")  # 默认值
        self.assertEqual(rules[0].device, "*")

    def test_duplicate_id_rejected(self) -> None:
        with self.assertRaises(ValueError):
            parse_rules([{"id": "a", "metric": "t", "threshold": 1},
                         {"id": "a", "metric": "t", "threshold": 2}])

    def test_bad_operator_rejected(self) -> None:
        with self.assertRaises(ValueError):
            parse_rules([{"id": "a", "metric": "t", "op": "==", "threshold": 1}])

    def test_bad_json_rejected(self) -> None:
        with self.assertRaises(Exception):
            parse_rules("{not json}")


if __name__ == "__main__":
    unittest.main()
