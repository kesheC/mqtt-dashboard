"""模拟器：造多设备多指标数据，走 MQTT 或直接打 HTTP，用来演示与压测."""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
import time
import urllib.request

METRICS = ("temp", "hum", "vib", "rssi")


def simulate_value(metric: str, t: float, phase: float, rng: random.Random) -> float:
    """低温慢漂 + 周期波动 + 噪声；vib 偶尔尖峰，方便触发告警演示."""
    if metric == "temp":
        base = 24 + 4 * math.sin(t / 30 + phase)
    elif metric == "hum":
        base = 55 + 12 * math.sin(t / 55 + phase)
    elif metric == "vib":
        base = 1.2 + 0.4 * math.sin(t / 9 + phase)
        if rng.random() < 0.02:  # 2% 概率尖峰，用来触发 >3 的规则
            base += rng.uniform(2.5, 4.5)
    else:
        base = -62 + 6 * math.sin(t / 17 + phase)
    return round(base + rng.gauss(0, 0.35), 3)


def run_http(api: str, devices: int, rate: float, duration: float, seed: int) -> int:
    rng = random.Random(seed)
    interval = 1.0 / rate
    sent = 0
    deadline = time.monotonic() + duration
    while time.monotonic() < deadline:
        t = time.monotonic()
        ts = int(time.time() * 1000)
        for i in range(devices):
            device = f"dev-{i + 1:02d}"
            body = json.dumps({
                "ts": ts,
                "metrics": {m: simulate_value(m, t, i * 1.7, rng) for m in METRICS},
            }).encode()
            url = f"{api}/api/ingest?device={device}"
            req = urllib.request.Request(url, data=body, method="POST",
                                         headers={"Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=5) as resp:
                    sent += 1
                    if resp.status not in (200, 202):
                        print(f"unexpected status {resp.status}", file=sys.stderr)
            except Exception as exc:
                print(f"post failed: {exc}", file=sys.stderr)
                return 1
        time.sleep(max(0.0, interval - (time.monotonic() - t)))
    print(f"sent {sent} messages ({devices} devices × {len(METRICS)} metrics × {duration:.0f}s)")
    return 0


def run_mqtt(host: str, port: int, devices: int, rate: float, duration: float, seed: int) -> int:
    import paho.mqtt.client as mqtt

    rng = random.Random(seed)
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="mqttdash-sim")
    client.connect(host, port, keepalive=30)
    client.loop_start()
    interval = 1.0 / rate
    sent = 0
    deadline = time.monotonic() + duration
    try:
        while time.monotonic() < deadline:
            t = time.monotonic()
            ts = int(time.time() * 1000)
            for i in range(devices):
                device = f"dev-{i + 1:02d}"
                payload = json.dumps({
                    "ts": ts,
                    "metrics": {m: simulate_value(m, t, i * 1.7, rng) for m in METRICS},
                })
                client.publish(f"devices/{device}/telemetry", payload, qos=1)
                sent += 1
            time.sleep(max(0.0, interval - (time.monotonic() - t)))
    finally:
        client.loop_stop()
        client.disconnect()
    print(f"published {sent} messages to {host}:{port}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="mqtt-dashboard 数据模拟器")
    p.add_argument("--api", default="http://127.0.0.1:8000", help="HTTP 模式的目标地址")
    p.add_argument("--mqtt-host", help="给了就走 MQTT 发布（需要 broker）")
    p.add_argument("--mqtt-port", type=int, default=1883)
    p.add_argument("--devices", type=int, default=3)
    p.add_argument("--rate", type=float, default=2.0, help="每秒轮数")
    p.add_argument("--duration", type=float, default=30.0, help="运行秒数，0=一直跑")
    p.add_argument("--seed", type=int, default=7)
    args = p.parse_args(argv)

    duration = args.duration if args.duration > 0 else 10_000.0
    if args.mqtt_host:
        return run_mqtt(args.mqtt_host, args.mqtt_port, args.devices, args.rate, duration, args.seed)
    return run_http(args.api, args.devices, args.rate, duration, args.seed)


if __name__ == "__main__":
    sys.exit(main())
