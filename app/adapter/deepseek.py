"""DeepSeek Adapter(p16/p17):唯一接触供应商 chunk 的地方。

四个必须处理的边界:
  01 chunk.choices 可能为空(末尾 usage-only chunk)
  02 delta.content 可能为 None / 空串(仅 role 元数据,丢弃 → 空 delta 不漂移 seq)
  03 tool_calls 分片到达,必须按 index 聚合,finish 后才完整产出
  04 文本结束 ≠ 业务任务完成(finish_reason 原样上抛,由 Loop 裁决)
"""
from __future__ import annotations

from typing import Any, AsyncIterator, Iterable

from openai import AsyncOpenAI

from .base import ModelStreamEvent


def chunk_to_events(chunk: Any, pending: dict[int, dict]) -> list[ModelStreamEvent]:
    """纯函数:单个供应商 chunk → 0..n 个内部事件。可脱离 SDK/网络单测。"""
    events: list[ModelStreamEvent] = []
    choice = chunk.choices[0] if getattr(chunk, "choices", None) else None

    delta = getattr(choice, "delta", None) if choice else None
    if delta is not None:
        if delta.content:  # ⚠02:None/空串直接丢弃
            events.append(ModelStreamEvent("text.delta", {"delta": delta.content}))
        for tc in delta.tool_calls or []:
            slot = pending.setdefault(tc.index, {"call_id": None, "name": "", "arguments": ""})
            if getattr(tc, "id", None):
                slot["call_id"] = tc.id
            fn = getattr(tc, "function", None)
            if fn is not None:
                if fn.name:
                    slot["name"] += fn.name
                if fn.arguments:
                    slot["arguments"] += fn.arguments

    if choice is not None and choice.finish_reason:
        if pending:  # ⚠03:arguments 聚合完整后才产出
            events.append(
                ModelStreamEvent(
                    "model.tool_calls",
                    {"calls": [pending[i] for i in sorted(pending)]},
                )
            )
            pending.clear()
        events.append(ModelStreamEvent("model.finished", {"finish_reason": choice.finish_reason}))

    usage = getattr(chunk, "usage", None)  # ⚠01:usage-only chunk
    if usage is not None:
        if hasattr(usage, "model_dump"):
            data = usage.model_dump()
        else:
            data = {
                k: getattr(usage, k)
                for k in ("prompt_tokens", "completion_tokens", "total_tokens")
                if hasattr(usage, k)
            }
        events.append(ModelStreamEvent("model.usage", {"usage": data}))

    return events


class DeepSeekAdapter:
    """满足 StreamingModel 协议;max_retries=0,重试决策权归 Harness(p16)。"""

    def __init__(
        self,
        api_key: str,
        base_url: str,
        model_name: str,
        *,
        max_tokens: int = 2048,
        timeout: float = 60.0,
    ) -> None:
        self.model = model_name
        self._max_tokens = max_tokens
        self._client = AsyncOpenAI(
            api_key=api_key,
            base_url=base_url,
            timeout=timeout,
            max_retries=0,
        )

    async def stream(
        self,
        messages: list[dict],
        tools: list[dict] | None = None,
    ) -> AsyncIterator[ModelStreamEvent]:
        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "stream": True,
            "max_tokens": self._max_tokens,
            "stream_options": {"include_usage": True},
        }
        if tools:
            kwargs["tools"] = tools

        stream = await self._client.chat.completions.create(**kwargs)
        pending: dict[int, dict] = {}
        async for chunk in stream:
            for event in chunk_to_events(chunk, pending):
                yield event
