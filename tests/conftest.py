"""测试公共设施:ScriptedModel 注入 + SSE 收集器。

create_app(model=...) 的依赖注入让测试不依赖真实 API Key;
ScriptedModel 只存在于测试,不进入运行时(应用只有 DeepSeekAdapter)。
"""
from __future__ import annotations

import json
from typing import Any

import httpx

from app.adapter.base import ModelStreamEvent
from app.config import GatewayConfig
from app.events import TERMINAL_EVENTS, RunEvent
from app.main import create_app
from app.store.memory import InMemoryRunStore


class ScriptedModel:
    """按 steps 依次响应每次 stream() 调用;Exception 项表示该次调用抛错。"""

    def __init__(self, steps: list[Any], model: str = "scripted") -> None:
        self.steps = list(steps)
        self.calls = 0
        self.model = model

    async def stream(self, messages: list[dict], tools: list[dict] | None = None):
        step = self.steps[min(self.calls, len(self.steps) - 1)]
        self.calls += 1
        if isinstance(step, Exception):
            raise step
        if hasattr(step, "__aiter__"):
            async for event in step:
                yield event
        else:
            for event in step:
                yield event


def make_cfg(**overrides) -> GatewayConfig:
    values = dict(
        deepseek_api_key="test-key",
        heartbeat_seconds=0.2,
        cancel_grace_seconds=0.4,
        max_retries=2,
    )
    values.update(overrides)
    return GatewayConfig(**values)


def make_app(steps: list[Any], **cfg_overrides):
    store = InMemoryRunStore()
    app = create_app(model=ScriptedModel(steps), store=store, cfg=make_cfg(**cfg_overrides))
    return app, store


def make_client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        timeout=httpx.Timeout(10, read=30),
    )


async def create_run(client: httpx.AsyncClient, task: str = "测试任务") -> str:
    resp = await client.post("/runs", json={"task": task})
    assert resp.status_code == 202, resp.text
    return resp.json()["run_id"]


async def collect_events(
    client: httpx.AsyncClient,
    run_id: str,
    *,
    last_event_id: int | None = None,
    max_events: int = 500,
) -> list[RunEvent]:
    """按 SSE 帧格式解析事件,直到收到终态或连接关闭。"""
    params = {} if last_event_id is None else {"last_event_id": last_event_id}
    events: list[RunEvent] = []
    current: dict[str, str] = {}
    async with client.stream("GET", f"/runs/{run_id}/events", params=params) as resp:
        assert resp.status_code == 200
        async for line in resp.aiter_lines():
            if line == "":
                if current:
                    event = RunEvent.model_validate(json.loads(current["data"]))
                    events.append(event)
                    current = {}
                    if event.type in TERMINAL_EVENTS or len(events) >= max_events:
                        break
            elif line.startswith(":"):
                continue
            else:
                key, _, value = line.partition(": ")
                if key == "data":
                    current["data"] = value
    return events
