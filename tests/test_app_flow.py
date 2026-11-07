"""端到端流程:创建 → 订阅 → 事件顺序/seq 连续 → 状态查询 → 断线重放。"""
from __future__ import annotations

import openai
import httpx as httpx_lib

from app.adapter.base import ModelStreamEvent
from tests.conftest import collect_events, create_run, make_app, make_client


def happy_steps():
    return [[
        ModelStreamEvent("text.delta", {"delta": "你好"}),
        ModelStreamEvent("text.delta", {"delta": ",世界"}),
        ModelStreamEvent("model.finished", {"finish_reason": "stop"}),
        ModelStreamEvent("model.usage", {"usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5}}),
    ]]


async def test_full_flow_event_order_and_seq():
    app, store = make_app(happy_steps())
    async with make_client(app) as client:
        run_id = await create_run(client)
        events = await collect_events(client, run_id)
        types = [e.type for e in events]
        assert types[0] == "run.started"
        assert types[-1] == "run.completed"
        assert types[1:3] == ["text.delta", "text.delta"]
        assert [e.seq for e in events] == list(range(len(events)))  # seq 从 0 连续递增
        deltas = [e.data["delta"] for e in events if e.type == "text.delta"]
        assert deltas == ["你好", ",世界"]

        summary = (await client.get(f"/runs/{run_id}")).json()
        assert summary["status"] == "completed"
        assert summary["ttft_ms"] is not None
        assert summary["terminal"]["usage"]["total_tokens"] == 5
        assert summary["retries"] == 0


async def test_replay_from_last_event_id_no_rerun():
    app, store = make_app(happy_steps())
    async with make_client(app) as client:
        run_id = await create_run(client)
        first_pass = await collect_events(client, run_id)
        assert first_pass[-1].type == "run.completed"
        assert len(first_pass) == 4  # started + 2×delta + completed

        # 断线续传:从 seq=1 之后补发,不重新调用模型(p36)
        replayed = await collect_events(client, run_id, last_event_id=1)
        assert [e.seq for e in replayed] == [2, 3]
        assert replayed[0].data["delta"] == ",世界"
        assert replayed[-1].type == "run.completed"

        # 审计重放:从头回放全部事实轨迹
        full = await collect_events(client, run_id, last_event_id=-1)
        assert [e.seq for e in full] == [0, 1, 2, 3]


async def test_retry_before_first_business_event():
    conn_error = openai.APIConnectionError(request=httpx_lib.Request("POST", "http://test"))
    steps = [
        conn_error,  # 第一次调用失败(尚未产生业务事件)
        [ModelStreamEvent("text.delta", {"delta": "ok"}),
         ModelStreamEvent("model.finished", {"finish_reason": "stop"})],
    ]
    app, _ = make_app(steps, max_retries=2)
    async with make_client(app) as client:
        run_id = await create_run(client)
        events = await collect_events(client, run_id)
        types = [e.type for e in events]
        assert "run.retrying" in types
        assert types.index("run.retrying") < types.index("text.delta")  # 重试在业务事件之前
        assert types[-1] == "run.completed"
        summary = (await client.get(f"/runs/{run_id}")).json()
        assert summary["retries"] == 1


async def test_permanent_error_fails_run_with_stable_code():
    from app.events import RunFailed

    app, _ = make_app([RunFailed("MODEL_AUTH", "401", retryable=False)])
    async with make_client(app) as client:
        run_id = await create_run(client)
        events = await collect_events(client, run_id)
        assert events[-1].type == "run.failed"
        assert events[-1].data["code"] == "MODEL_AUTH"
        assert events[-1].data["retryable"] is False


async def test_events_of_unknown_run_404():
    app, _ = make_app(happy_steps())
    async with make_client(app) as client:
        assert (await client.get("/runs/unknown/events")).status_code == 404
        assert (await client.get("/runs/unknown")).status_code == 404
        assert (await client.post("/runs/unknown/cancel")).status_code == 404


async def test_health_and_index_page():
    app, _ = make_app(happy_steps())
    async with make_client(app) as client:
        assert (await client.get("/healthz")).json() == {"ok": True}
        page = await client.get("/")
        assert page.status_code == 200
        assert "Streaming Agent Gateway" in page.text
