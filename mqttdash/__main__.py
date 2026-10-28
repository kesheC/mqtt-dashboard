"""启动入口：``python -m mqttdash``.

环境变量
--------
DB_PATH       SQLite 文件路径（默认 mqttdash.db）
PORT          HTTP 端口（默认 8000）
MQTT_HOST     给了就启用 MQTT 订阅（默认关闭，只用 HTTP 上报）
MQTT_PORT     broker 端口（默认 1883）
MQTT_USER / MQTT_PASS
RULES         规则 JSON 字符串；也可以给 RULES_FILE 指向文件
RETENTION_S   数据保留秒数（默认 604800 = 7 天），0 表示不清理
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from .api import create_app
from .ingest import Pipeline
from .rules import RuleEngine, parse_rules
from .store import Store

DEFAULT_RULES = [
    {"id": "temp-high", "metric": "temp", "op": ">", "threshold": 30,
     "duration_s": 3, "cooldown_s": 30, "severity": "warning"},
    {"id": "temp-critical", "metric": "temp", "op": ">", "threshold": 33,
     "duration_s": 1, "cooldown_s": 30, "severity": "critical"},
    {"id": "vib-spike", "metric": "vib", "op": ">", "threshold": 2.5,
     "duration_s": 0, "cooldown_s": 10, "severity": "info"},
    {"id": "hum-low", "metric": "hum", "op": "<", "threshold": 40,
     "duration_s": 5, "cooldown_s": 60, "severity": "warning"},
]

log = logging.getLogger("mqttdash")


def load_rules():
    raw = os.environ.get("RULES")
    if raw:
        return parse_rules(raw)
    path = os.environ.get("RULES_FILE")
    if path:
        return parse_rules(Path(path).read_text(encoding="utf-8"))
    return parse_rules(DEFAULT_RULES)


def build() -> tuple:
    store = Store(os.environ.get("DB_PATH", "mqttdash.db"))
    engine = RuleEngine(rules=load_rules())
    pipeline = Pipeline(store, engine)

    mqtt = None
    host = os.environ.get("MQTT_HOST", "").strip()
    if host:
        from .mqtt import MqttIngestor

        mqtt = MqttIngestor(
            pipeline,
            host=host,
            port=int(os.environ.get("MQTT_PORT", "1883")),
            username=os.environ.get("MQTT_USER") or None,
            password=os.environ.get("MQTT_PASS") or None,
        )
    return store, pipeline, mqtt


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    store, pipeline, mqtt = build()
    if mqtt is not None:
        mqtt.start()
        log.info("mqtt ingestion enabled: %s:%s", mqtt.host, mqtt.port)
    else:
        log.info("mqtt ingestion disabled (仅 HTTP /api/ingest)")

    app = create_app(store, pipeline, mqtt)

    @app.on_event("shutdown")
    def _shutdown() -> None:  # pragma: no cover - 由 uvicorn 触发
        if mqtt is not None:
            mqtt.stop()
        store.close()

    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    log.info("http://127.0.0.1:%d  (docs: /api/docs)", port)
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
    return 0


if __name__ == "__main__":
    sys.exit(main())
