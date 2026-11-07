"""Run Store 契约(p19):任务状态不得依赖单次 HTTP 请求的生命周期。

内存实现先跑通事件协议;生产环境按同一接口替换为
PostgreSQL(事实,UNIQUE(run_id, seq))+ Redis(通知,可丢)(p37)。
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Optional, Protocol

from ..events import TERMINAL_EVENTS, RunEvent

CREATED = "created"
RUNNING = "running"
CANCELLING = "cancelling"
COMPLETED = "completed"
FAILED = "failed"
CANCELLED = "cancelled"

TERMINAL_STATUSES = {COMPLETED, FAILED, CANCELLED}

EVENT_TO_STATUS = {
    "run.completed": COMPLETED,
    "run.failed": FAILED,
    "run.cancelled": CANCELLED,
}


@dataclass
class Run:
    run_id: str
    task: str
    trace_id: str
    model: str
    status: str = CREATED
    created_at: str = ""
    started_perf: float | None = None
    ttft_ms: float | None = None
    retries: int = 0
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    events: list[RunEvent] = field(default_factory=list)
    seq: int = -1  # 最后已分配的 seq
    handle: Optional[asyncio.Task] = None
    archive: dict | None = None  # p32:取消/失败时的部分输出存档

    @property
    def terminal(self) -> Optional[RunEvent]:
        if self.events and self.events[-1].type in TERMINAL_EVENTS:
            return self.events[-1]
        return None


class RunStore(Protocol):
    def create_run(self, task: str, model: str) -> Run: ...
    def get(self, run_id: str) -> Run | None: ...
    def attach_task(self, run_id: str, handle: asyncio.Task) -> None: ...
    def mark_running(self, run_id: str) -> None: ...
    async def publish(self, run_id: str, type_: str, data: dict) -> Optional[RunEvent]: ...
    def events_after(self, run_id: str, after_seq: int) -> list[RunEvent]: ...
    async def wait_notify(self, run_id: str, timeout: float) -> bool: ...
    def is_terminal(self, run_id: str) -> bool: ...
    def request_cancel(self, run_id: str) -> bool: ...
