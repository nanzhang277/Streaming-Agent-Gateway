"""FastAPI 组装:创建 / 订阅 / 取消 三接口分离(p11/p21)。

curl 示例见 README。
"""
from __future__ import annotations

import asyncio
import logging
from collections import Counter
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .adapter import StreamingModel
from .adapter.deepseek import DeepSeekAdapter
from .config import GatewayConfig
from .events import TERMINAL_EVENTS
from .gateway import build_events_route
from .loop import AgentLoop
from .store.base import TERMINAL_STATUSES
from .store.memory import InMemoryRunStore

log = logging.getLogger("gateway")

_STATIC = Path(__file__).parent / "static"


class CreateRunRequest(BaseModel):
    task: str = Field(min_length=1, max_length=8000)


def create_app(
    model: StreamingModel | None = None,
    store: InMemoryRunStore | None = None,
    cfg: GatewayConfig | None = None,
) -> FastAPI:
    cfg = cfg or GatewayConfig.from_env()
    store = store or InMemoryRunStore()
    if model is None:
        if not cfg.deepseek_api_key:
            raise RuntimeError(
                "缺少 DEEPSEEK_API_KEY:复制 .env.example 为 .env 并填写后再启动"
            )
        model = DeepSeekAdapter(
            api_key=cfg.deepseek_api_key,
            base_url=cfg.deepseek_base_url,
            model_name=cfg.deepseek_model,
        )

    app = FastAPI(title="Streaming Agent Gateway", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["*"],
    )

    # ---- CREATE:创建后台任务,立即返回(p21)----

    @app.post("/runs", status_code=202)
    async def create_run(body: CreateRunRequest):
        run = store.create_run(body.task, model=getattr(model, "model", "unknown"))
        loop = AgentLoop(store, model, cfg)
        handle = asyncio.create_task(loop.run(run.run_id, body.task), name=f"run:{run.run_id}")
        handle.add_done_callback(lambda t: _log_crash(store, t))
        store.attach_task(run.run_id, handle)
        return {
            "run_id": run.run_id,
            "trace_id": run.trace_id,
            "status": run.status,
            "subscribe": f"/runs/{run.run_id}/events",
        }

    # ---- SUBSCRIBE:订阅事件流 ----

    app.add_api_route(
        "/runs/{run_id}/events",
        build_events_route(store, cfg),
        methods=["GET"],
        summary="SSE 订阅;支持 Last-Event-ID 断线续传",
    )

    # ---- 状态查询(审计)----

    @app.get("/runs/{run_id}")
    async def run_summary(run_id: str):
        run = store.get(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="run 不存在")
        counts = Counter(e.type for e in run.events)
        terminal = run.terminal
        return {
            "run_id": run.run_id,
            "trace_id": run.trace_id,
            "task": run.task,
            "model": run.model,
            "status": run.status,
            "created_at": run.created_at,
            "last_seq": run.seq,
            "event_count": len(run.events),
            "event_counts": dict(counts),
            "ttft_ms": run.ttft_ms,
            "retries": run.retries,
            "tool_calls": run.archive["tool_state"] if run.archive else [],
            "terminal": terminal.data if terminal else None,
            "terminal_type": terminal.type if terminal else None,
            "archive": run.archive,
        }

    # ---- CONTROL:显式取消(p29/p31)----

    @app.post("/runs/{run_id}/cancel", status_code=202)
    async def cancel_run(run_id: str):
        run = store.get(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="run 不存在")
        if run.status in TERMINAL_STATUSES:
            raise HTTPException(
                status_code=409, detail={"message": "run 已终止", "status": run.status}
            )
        store.request_cancel(run_id)  # created/running → cancelling

        async def _watchdog():
            # p29 升级链:协作式未退出 → 强制 task.cancel()
            await asyncio.sleep(cfg.cancel_grace_seconds)
            fresh = store.get(run_id)
            if fresh is None or store.is_terminal(run_id):
                return
            if fresh.handle and not fresh.handle.done():
                log.warning("run %s 取消宽限期已过,强制 task.cancel()", run_id)
                fresh.handle.cancel()

        asyncio.create_task(_watchdog())
        return {"run_id": run_id, "status": run.status}

    # ---- 静态测试页 ----

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    @app.get("/")
    async def index():
        return FileResponse(_STATIC / "index.html")

    return app


def _log_crash(store: InMemoryRunStore, task: asyncio.Task) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        log.error("run task crashed: %r", exc)
