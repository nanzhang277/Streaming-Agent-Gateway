"""SSE Gateway(p13 层 05):编码、心跳、订阅管理。

职责边界:Gateway 只传输事实,不决定任务是否完成;
断开订阅 ≠ 取消 Run —— gen 里绝不触发取消(p21/p28)。
"""
from __future__ import annotations

from fastapi import HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from .config import GatewayConfig
from .sse import SSE_HEADERS, encode_heartbeat, encode_sse
from .store.memory import InMemoryRunStore


def build_events_route(store: InMemoryRunStore, cfg: GatewayConfig):
    async def subscribe(
        run_id: str,
        request: Request,
        last_event_id: int | None = Query(
            default=None,
            alias="last_event_id",
            description="断线续传:客户端最后收到的 seq;优先于 Last-Event-ID 头",
        ),
    ):
        if store.get(run_id) is None:
            raise HTTPException(status_code=404, detail="run 不存在")

        raw = last_event_id if last_event_id is not None else request.headers.get("last-event-id")
        try:
            after = int(raw) if raw is not None else -1
        except (TypeError, ValueError):
            after = -1

        async def gen():
            # retry: 建议浏览器自动重连等待毫秒(p10)
            yield b"retry: 3000\n\n"
            seq = after
            while True:
                if await request.is_disconnected():
                    return
                # 先补发缺失事件(断线续传恢复的是 Harness Event,不是模型 Token,p36)
                for event in store.events_after(run_id, seq):
                    yield encode_sse(event)
                    seq = event.seq
                if store.is_terminal(run_id):
                    return
                if not await store.wait_notify(run_id, cfg.heartbeat_seconds):
                    yield encode_heartbeat()

        return StreamingResponse(gen(), headers=SSE_HEADERS)

    return subscribe
