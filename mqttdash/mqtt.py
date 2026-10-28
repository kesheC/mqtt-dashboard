"""MQTT 订阅线程：断线自动重连，消息按 topic 里的设备名入线，坏消息计数但不拖垮线程."""

from __future__ import annotations

import logging
import threading
import time

from .ingest import PayloadError, Pipeline

log = logging.getLogger("mqttdash.mqtt")

TOPIC_PREFIX = "devices"


def device_from_topic(topic: str) -> str | None:
    """devices/<device>/telemetry → <device>；不符合约定返回 None."""
    parts = topic.split("/")
    if len(parts) == 3 and parts[0] == TOPIC_PREFIX and parts[2] == "telemetry":
        return parts[1]
    return None


class MqttIngestor:
    """paho-mqtt 的薄封装：连接/重连/回调都收在这里，业务只看到 Pipeline."""

    def __init__(self, pipeline: Pipeline, host: str = "127.0.0.1", port: int = 1883,
                 topic: str = f"{TOPIC_PREFIX}/+/telemetry",
                 username: str | None = None, password: str | None = None) -> None:
        self.pipeline = pipeline
        self.host = host
        self.port = port
        self.topic = topic
        self.username = username
        self.password = password
        self._thread: threading.Thread | None = None
        self._client = None
        self._stop = threading.Event()
        self._connected = False
        self._reconnects = 0
        self._messages = 0
        self._errors = 0
        self._last_error: str | None = None

    # ---- 生命周期 ----

    def start(self) -> None:
        import paho.mqtt.client as mqtt

        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="mqttdash")
        if self.username:
            client.username_pw_set(self.username, self.password)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        client.reconnect_delay_set(min_delay=1, max_delay=30)
        self._client = client

        def run() -> None:
            while not self._stop.is_set():
                try:
                    client.connect(self.host, self.port, keepalive=30)
                    client.loop_forever(retry_first_connection=False)
                except Exception as exc:  # broker 没起来：别让进程死掉
                    self._last_error = str(exc)
                    log.warning("mqtt connect failed: %s", exc)
                    self._stop.wait(3.0)

        self._thread = threading.Thread(target=run, name="mqtt", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._client is not None:
            try:
                self._client.disconnect()
            except Exception:
                pass
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    # ---- 回调 ----

    def _on_connect(self, client, userdata, flags, reason_code, properties=None) -> None:
        self._connected = True
        client.subscribe(self.topic, qos=1)
        log.info("mqtt connected %s:%s, subscribed %s", self.host, self.port, self.topic)

    def _on_disconnect(self, client, userdata, *args) -> None:
        self._connected = False
        self._reconnects += 1

    def _on_message(self, client, userdata, msg) -> None:
        device = device_from_topic(msg.topic)
        if device is None:
            self._errors += 1
            self._last_error = f"unexpected topic {msg.topic}"
            return
        try:
            self.pipeline.ingest_raw(device, msg.payload)
            self._messages += 1
        except PayloadError as exc:
            self._errors += 1
            self._last_error = str(exc)
        except Exception as exc:  # 单条消息炸掉不能停订阅
            self._errors += 1
            self._last_error = repr(exc)

    # ---- 观测 ----

    def status(self) -> dict:
        return {
            "enabled": True,
            "connected": self._connected,
            "host": self.host,
            "port": self.port,
            "topic": self.topic,
            "messages": self._messages,
            "errors": self._errors,
            "reconnects": self._reconnects,
            "last_error": self._last_error,
            "uptime_note": "订阅线程 daemon，断线 3s 后重试",
            "checked_at": int(time.time() * 1000),
        }
