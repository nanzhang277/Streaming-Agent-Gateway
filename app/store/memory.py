"""内存 Run Store:单一终态守卫和 CAS 取消都收敛在这里。

生产替换为 PostgreSQL + Redis 时,以下方法签名不变(p19/p37):
  - publish → INSERT run_events(UNIQUE(run_id, seq))+ Redis 通知
  - events_after → SELECT ... WHERE seq > :after ORDER BY seq
  - wait_notify → 阻塞读 Redis Stream / PubSub
"""
from __future__ import annotations

import asyncio
import time
import uuid

from ..events import RunEvent, utc_now_iso
from .base import (
    CANCELLING,
    CREATED,
    EVENT_TO_STATUS,
    RUNNING,
    TERMINAL_STATUSES,
    Run,
)


class UnknownRun(KeyError):
    pass


class InMemoryRunStore:
    def __init__(self) -> None:
        self._runs: dict[str, Run] = {}
        self._cond = asyncio.Condition()

    # ---- 命令 ----

    def create_run(self, task: str, model: str) -> Run:
        run = Run(
            run_id=uuid.uuid4().hex[:12],
            trace_id=uuid.uuid4().hex,
            task=task,
            model=model,
            created_at=utc_now_iso(),
        )
        self._runs[run.run_id] = run
        return run

    def attach_task(self, run_id: str, handle: asyncio.Task) -> None:
        self._require(run_id).handle = handle

    def mark_running(self, run_id: str) -> None:
        run = self._require(run_id)
        if run.status == CREATED:
            run.status = RUNNING
        run.started_perf = time.monotonic()

    async def publish(self, run_id: str, type_: str, data: dict) -> RunEvent | None:
        run = self._require(run_id)
        if run.status in TERMINAL_STATUSES:
            return None  # 单一终态守卫:终态之后的事实一律丢弃
        event = RunEvent(run_id=run_id, seq=run.seq + 1, type=type_, data=data)  # type: ignore[arg-type]
        run.seq = event.seq
        run.events.append(event)
        if type_ == "run.retrying":
            run.retries += 1
        if type_ in EVENT_TO_STATUS:
            run.status = EVENT_TO_STATUS[type_]
        async with self._cond:
            self._cond.notify_all()
        return event

    def request_cancel(self, run_id: str) -> bool:
        """CAS:仅 created/running → cancelling(p31),幂等安全。"""
        run = self._require(run_id)
        if run.status in TERMINAL_STATUSES or run.status == CANCELLING:
            return False
        run.status = CANCELLING
        run.cancel_event.set()
        return True

    # ---- 查询 ----

    def get(self, run_id: str) -> Run | None:
        return self._runs.get(run_id)

    def events_after(self, run_id: str, after_seq: int) -> list[RunEvent]:
        run = self._require(run_id)
        return [e for e in run.events if e.seq > after_seq]

    def is_terminal(self, run_id: str) -> bool:
        return self._require(run_id).status in TERMINAL_STATUSES

    async def wait_notify(self, run_id: str, timeout: float) -> bool:
        self._require(run_id)
        async with self._cond:
            try:
                await asyncio.wait_for(self._cond.wait(), timeout)
                return True
            except asyncio.TimeoutError:
                return False

    def _require(self, run_id: str) -> Run:
        run = self._runs.get(run_id)
        if run is None:
            raise UnknownRun(run_id)
        return run
