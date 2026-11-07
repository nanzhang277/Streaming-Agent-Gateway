"""取消链路:请求取消 → cancelling 状态 → 协作/强制升级 → 唯一终态(p29-p31)。"""
from __future__ import annotations

import asyncio

from app.adapter.base import ModelStreamEvent
from tests.conftest import collect_events, create_run, make_app, make_client


def steps_never_finish():
    async def slow_stream():
        yield ModelStreamEvent("text.delta", {"delta": "开头"})
        await asyncio.sleep(30)  # 永远等不到下一步,只能靠取消结束
    return [slow_stream()]


async def test_cancel_produces_single_cancelled_terminal_and_archive():
    app, store = make_app(steps_never_finish(), cancel_grace_seconds=0.3)
    async with make_client(app) as client:
        run_id = await create_run(client, task="长任务")

        collected = asyncio.create_task(collect_events(client, run_id))
        await asyncio.sleep(0.15)  # 让首个 text.delta 先到达(已产生业务事件)

        resp = await client.post(f"/runs/{run_id}/cancel")
        assert resp.status_code == 202
        assert resp.json()["status"] == "cancelling"  # p31:不直接跳终态

        events = await asyncio.wait_for(collected, timeout=10)
        types = [e.type for e in events]
        assert "text.delta" in types
        assert types[-1] == "run.cancelled"          # 唯一终态
        assert types.count("run.cancelled") == 1
        assert events[-1].data["reason"] == "forced_task_cancel"
        assert events[-1].data["last_seq"] >= 1

        # p32:取消即存档点 —— 部分输出被保存
        summary = (await client.get(f"/runs/{run_id}")).json()
        assert summary["status"] == "cancelled"
        assert summary["archive"]["partial_text"].endswith("开头")
        assert summary["archive"]["last_seq"] == events[-1].data["last_seq"]

        # 终态后再取消 → 409
        assert (await client.post(f"/runs/{run_id}/cancel")).status_code == 409


async def test_cancel_twice_while_cancelling_is_idempotent():
    app, _ = make_app(steps_never_finish(), cancel_grace_seconds=30.0)
    async with make_client(app) as client:
        run_id = await create_run(client)
        first = await client.post(f"/runs/{run_id}/cancel")
        second = await client.post(f"/runs/{run_id}/cancel")
        assert first.status_code == 202
        assert second.status_code == 202  # cancelling 中重复请求不报错
        assert second.json()["status"] == "cancelling"
