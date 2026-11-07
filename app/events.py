"""Harness Event 协议(p14):run_id + seq 是全系统的事件位置键。

任何组件发布事件前都经过 RunEvent 校验;客户端遇到未知 type
应忽略或降级,而不是崩溃 —— schema_version 是协议演化护栏。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field

EventType = Literal[
    "run.started",
    "text.delta",
    "tool.started",
    "tool.completed",
    "run.retrying",
    "run.completed",
    "run.failed",
    "run.cancelled",
]

# 一个 Run 只有一个终态;终态事件是完成/取消竞态的最终裁判(p7)
TERMINAL_EVENTS: set[str] = {"run.completed", "run.failed", "run.cancelled"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class RunEvent(BaseModel):
    schema_version: Literal["1"] = "1"
    run_id: str
    seq: int = Field(ge=0)
    type: EventType
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    data: dict[str, Any] = Field(default_factory=dict)


class RunCancelled(Exception):
    """协作式取消的控制信号,不是普通异常(p20)。"""


class RunFailed(Exception):
    def __init__(self, code: str, message: str, *, retryable: bool = False, hint: str = "") -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.hint = hint
