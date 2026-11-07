"""SSE 帧协议边界(p10/p15):编码是纯函数,必须可单测。

心跳是注释行,不进入事件语义、不占业务 seq;
Content-Type 必须是 text/event-stream,否则 EventSource 拒绝连接。
"""
from __future__ import annotations

import json

from .events import RunEvent


def encode_sse(event: RunEvent) -> bytes:
    payload = event.model_dump(mode="json")
    lines = [
        f"id: {event.seq}",
        f"event: {event.type}",
        "data: " + json.dumps(payload, ensure_ascii=False),
        "",
        "",
    ]
    return "\n".join(lines).encode("utf-8")


def encode_heartbeat() -> bytes:
    return b": heartbeat\n\n"


SSE_HEADERS = {
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache",
    # p22 陷阱3:禁止 Nginx 等反向代理重新聚合响应
    "X-Accel-Buffering": "no",
}
