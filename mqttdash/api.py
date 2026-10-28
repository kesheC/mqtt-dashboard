"""HTTP API：入站上报 + 看板查询。规则与限流参数全部来自环境变量，方便容器化部署."""

from __future__ import annotations

import time
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from .ingest import PayloadError, Pipeline
from .mqtt import MqttIngestor
from .store import Store

MAX_BODY_BYTES = 64 * 1024


def create_app(store: Store, pipeline: Pipeline,
               mqtt: MqttIngestor | None = None) -> FastAPI:
    app = FastAPI(title="mqtt-dashboard", version="0.1.0", docs_url="/api/docs",
                  openapi_url="/api/openapi.json")

    def get_pipeline() -> Pipeline:  # 便于测试里覆盖
        return pipeline

    @app.get("/healthz")
    def healthz() -> dict[str, Any]:
        return {
            "ok": True,
            "mqtt": None if mqtt is None else mqtt.status(),
            "ingest": pipeline.stats.as_dict(),
        }

    @app.get("/api/stats")
    def stats() -> dict[str, Any]:
        data = store.stats()
        data["mqtt"] = None if mqtt is None else mqtt.status()
        data["ingest"] = pipeline.stats.as_dict()
        data["active_alerts_engine"] = pipeline.engine.active_count()
        return data

    @app.get("/api/devices")
    def devices() -> list[dict[str, Any]]:
        out = []
        for row in store.devices():
            out.append({
                "device_id": row.device_id,
                "metrics": store.metrics(row.device_id),
                "samples": row.samples,
                "first_ts": row.first_ts,
                "last_ts": row.last_ts,
                "age_s": round((int(time.time() * 1000) - row.last_ts) / 1000, 1),
            })
        return out

    @app.get("/api/series")
    def series(
        device: str = Query(..., min_length=1, max_length=128),
        metric: str = Query(..., min_length=1, max_length=128),
        window: int = Query(900, ge=10, le=86_400, description="回看多少秒"),
        bucket: int = Query(30, ge=0, le=3600, description="聚合桶宽（秒），0=原始点"),
    ) -> dict[str, Any]:
        points = store.series(device, metric, window_s=window, bucket_s=bucket)
        return {
            "device": device,
            "metric": metric,
            "window_s": window,
            "bucket_s": bucket,
            "points": [
                {"ts": p.ts, "avg": round(p.avg, 4), "min": round(p.min, 4),
                 "max": round(p.max, 4), "count": p.count}
                for p in points
            ],
        }

    @app.get("/api/alerts")
    def alerts(limit: int = Query(50, ge=1, le=500),
               only_active: bool = False) -> list[dict[str, Any]]:
        return [
            {"id": a.id, "rule_id": a.rule_id, "device_id": a.device_id,
             "metric": a.metric, "severity": a.severity, "value": a.value,
             "threshold": a.threshold, "started_ts": a.started_ts,
             "resolved_ts": a.resolved_ts, "message": a.message}
            for a in store.alerts(limit=limit, only_active=only_active)
        ]

    @app.get("/api/events")
    def events(limit: int = Query(20, ge=1, le=200)) -> list[dict[str, Any]]:
        return pipeline.recent_events(limit)

    @app.get("/api/rules")
    def rules() -> list[dict[str, Any]]:
        return [
            {"id": r.id, "device": r.device, "metric": r.metric, "op": r.op,
             "threshold": r.threshold, "duration_s": r.duration_s,
             "cooldown_s": r.cooldown_s, "severity": r.severity}
            for r in pipeline.engine.rules
        ]

    @app.post("/api/ingest")
    async def ingest(request: Request,
                     device: str = Query(..., min_length=1, max_length=128),
                     pipeline: Pipeline = Depends(get_pipeline)) -> JSONResponse:
        body = await request.body()
        if len(body) > MAX_BODY_BYTES:
            raise HTTPException(status_code=413, detail="payload too large")
        try:
            written = pipeline.ingest_raw(device, body)
        except PayloadError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse({"ok": True, "written": written}, status_code=202)

    app.mount("/", StaticFiles(directory="static", html=True), name="static")
    return app
