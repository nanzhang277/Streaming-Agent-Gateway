"""Model Adapter 协议(p18):供应商协议止步于此。

Adapter 产生内部事件 ModelStreamEvent,不产生 SSE 字符串;
更换模型只增加 Adapter,Loop / Store / Client 不变。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Protocol


@dataclass(frozen=True)
class ModelStreamEvent:
    type: str  # text.delta / model.tool_calls / model.finished / model.usage
    data: dict[str, Any] = field(default_factory=dict)


class StreamingModel(Protocol):
    model: str

    def stream(
        self,
        messages: list[dict],
        tools: list[dict] | None = None,
    ) -> AsyncIterator[ModelStreamEvent]: ...
