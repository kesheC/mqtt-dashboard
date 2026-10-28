# mqtt-dashboard · 设备遥测看板

[![CI](https://github.com/kesheC/mqtt-dashboard/actions/workflows/ci.yml/badge.svg)](https://github.com/kesheC/mqtt-dashboard/actions/workflows/ci.yml)

一个完整的 IoT 数据链路：**MQTT / HTTP 入站 → 校验 → SQLite 落库 → 阈值规则引擎 → 看板**。前端无构建（原生 JS + canvas），后端 FastAPI，一条 `docker compose up` 就能跑，没有 broker 也能用 HTTP 上报演示。

![看板截图](docs/preview.png)

*截图是真实运行的看板：3 台设备各自 4 个指标，10 秒聚合的 min/avg/max 带，右侧是规则触发的告警（`vib` 尖峰触发、回落自动恢复）。*

## 架构

```
 设备 / 模拟器
   │  MQTT: devices/<id>/telemetry        HTTP: POST /api/ingest?device=<id>
   ▼                                        ▼
 ┌────────────────────── mqttdash ─────────────────────────────────┐
 │ MqttIngestor (paho, 断线 3s 重连)        api.Pipeline.ingest_raw  │
 │        └──────────────┬──────────────────────────┘              │
 │                parse_payload()  校验：json/数值/时间戳/指标数上限   │
 │                       ▼                                          │
 │                 Store.insert_many()  SQLite WAL + 批量           │
 │                       ▼                                          │
 │            RuleEngine.evaluate()  持续时长 + 冷却 + 恢复           │
 │                       ▼                                          │
 │        alerts 表（started_ts / resolved_ts）                      │
 │                       ▼                                          │
 │  GET /api/{stats,devices,series,alerts,rules,events}  → 静态看板   │
 └─────────────────────────────────────────────────────────────────┘
```

## 快速开始

```bash
pip install -r requirements.txt

# 1. 起服务（不配 MQTT_HOST 时只用 HTTP 上报）
python -m mqttdash                     # http://127.0.0.1:8000

# 2. 造数据（另开一个终端，3 台设备 × 4 指标 × 每秒 2 轮）
python -m mqttdash.sim --devices 3 --rate 2 --duration 60

# 3. 打开看板：http://127.0.0.1:8000/    API 文档：/api/docs
```

有 broker 时（或者直接用 compose）：

```bash
# 本地 mosquitto：启动模拟器走 MQTT 发布
MQTT_HOST=127.0.0.1 python -m mqttdash
python -m mqttdash.sim --mqtt-host 127.0.0.1 --duration 60

# 或者一把梭：mqtt broker + 应用 + 模拟器
docker compose up --build
```

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/ingest?device=<id>` | 上报。body 支持 `{"ts":ms,"metrics":{...}}`、扁平 `{"temp":25}`、裸数值（需带 ts） |
| GET | `/api/stats` | 总样本数、设备数、每分钟吞吐、活跃告警数、MQTT 状态 |
| GET | `/api/devices` | 设备列表：指标、样本数、最后上报距今 |
| GET | `/api/series?device=&metric=&window=900&bucket=30` | 时间桶聚合（`bucket=0` 返回原始点） |
| GET | `/api/alerts?limit=50&only_active=false` | 告警历史（含恢复时间） |
| GET | `/api/rules` | 当前生效的规则 |
| GET | `/api/events` | 最近触发/恢复的事件（内存环形缓冲，供调试） |
| GET | `/healthz` | 存活探测：MQTT 状态 + 入站统计 |
## 实现要点

- **入站自己做校验**：MQTT 上谁都能发消息，`parse_payload` 拒绝非 JSON、非数值、`NaN`/`Infinity`、布尔值、缺设备名、超过 64 个指标、非 UTF-8；失败计入 `stats.rejected` 并返回 400，不静默吞掉。
- **降采样放在 SQL 里**：3 设备 × 4 指标 × 600 秒全查出来再在 JS 里平均，等于把几万行搬过网络。用 `(ts / bucket_ms) * bucket_ms` 分桶、`AVG/MIN/MAX/COUNT` 聚合，只回传几十个点。注意 `GROUP BY` 要按别名 `bucket_ts` 分组——直接写 `GROUP BY ts` 会绑定表列，聚合静默失效。
- **桶按绝对时间对齐**：对齐 epoch 整数倍后每次刷新桶边界一致，图不会左右抖动。
- **规则用「持续时长 + 冷却 + 恢复」**：只要持续时长会被单点毛刺触发；只有阈值会在临界值附近反复 fire/resolve 形成告警风暴；只 fire 不 resolve 会让库里全是关不掉的告警。三者都要，并且恢复时同样回调更新 `resolved_ts`。
- **SQLite WAL + 单连接加锁**：写方是 MQTT 线程和 HTTP 线程，读方是看板轮询；WAL 让读不阻塞写，单写者配锁比连接池简单，代价是吞吐上限（到不了一万点/秒，那就该换时序库了）。
- **前端不引框架**：三块内容，原生 JS + canvas 够用，零构建、首屏就是 HTML；`fetch` 两秒一次，失败时页脚显示连续失败次数。

## 测试

48 个用例分四层：存储（桶聚合与对齐、窗口过滤、告警写入/恢复幂等、统计、清理）、规则引擎（阈值、持续时长、毛刺重置、冷却、多设备隔离、配置校验）、入站管线（9 种非法 payload 全部拒收且不落库、时间戳升级、NaN 拒绝、指标上限）、HTTP（400/422、告警与规则可见性、索引页、404）。

```bash
python -m unittest discover -s tests -t .
# Ran 48 tests ... OK
```

## 已知边界

- 单机 SQLite：多副本各写各的库，横向扩展要换 PostgreSQL/TimescaleDB。
- 规则只有阈值，没有窗口函数（"5 分钟内 3 次"）与通知渠道。
- 历史数据默认全留，`RETENTION_S` 需要在外部定时调用 `Store.prune`（未内置调度器）。
- 看板是轮询而非 WebSocket：2 秒延迟、固定 QPS。
- 没有鉴权：`/api/ingest` 与看板都假设内网，公网部署要加网关。
