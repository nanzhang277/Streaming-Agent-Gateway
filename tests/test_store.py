"""Store 测试:seq 单调、单一终态守卫、CAS 取消、重放过滤。"""
from __future__ import annotations

import pytest

from app.store.base import CANCELLED, COMPLETED, CANCELLING, CREATED, RUNNING
from app.store.memory import InMemoryRunStore, UnknownRun


def make_store() -> InMemoryRunStore:
    store = InMemoryRunStore()
    return store


async def test_seq_monotonic_and_events_after():
    store = make_store()
    run = store.create_run("task", model="m")
    for type_ in ("run.started", "text.delta", "text.delta"):
        await store.publish(run.run_id, type_, {"delta": "x"})
    assert run.seq == 2
    assert [e.seq for e in store.events_after(run.run_id, -1)] == [0, 1, 2]
    assert [e.seq for e in store.events_after(run.run_id, 0)] == [1, 2]
    assert store.events_after(run.run_id, 99) == []


async def test_single_terminal_guard():
    store = make_store()
    run = store.create_run("task", model="m")
    first = await store.publish(run.run_id, "run.completed", {"usage": {}})
    assert first is not None
    assert store.is_terminal(run.run_id)
    # 竞态下第二个终态被静默丢弃 → Run 只有一个终态(p7)
    assert await store.publish(run.run_id, "run.cancelled", {}) is None
    assert await store.publish(run.run_id, "text.delta", {"delta": "late"}) is None
    assert run.terminal is not None and run.terminal.type == "run.completed"
    assert run.status == COMPLETED


async def test_cancel_cas_transition():
    store = make_store()
    run = store.create_run("task", model="m")
    assert run.status == CREATED
    assert store.request_cancel(run.run_id) is True
    assert run.status == CANCELLING and run.cancel_event.is_set()
    # cancelling 后再次请求:幂等拒绝(取消中的 Run 不需要重复 CAS)
    assert store.request_cancel(run.run_id) is False
    await store.publish(run.run_id, "run.cancelled", {})
    assert store.request_cancel(run.run_id) is False
    assert run.status == CANCELLED


async def test_unknown_run_raises():
    store = make_store()
    with pytest.raises(UnknownRun):
        store.events_after("nope", 0)


async def test_retries_counter():
    store = make_store()
    run = store.create_run("task", model="m")
    await store.publish(run.run_id, "run.retrying", {"attempt": 1})
    await store.publish(run.run_id, "run.retrying", {"attempt": 2})
    assert run.retries == 2
