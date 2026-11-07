"""DeepSeek Adapter 纯函数测试:无需 SDK 对象与网络(p17 的 4 类边界)。"""
from __future__ import annotations

from types import SimpleNamespace as NS

from app.adapter.deepseek import chunk_to_events


def text_chunk(content, finish=None):
    return NS(choices=[NS(delta=NS(content=content, tool_calls=None), finish_reason=finish)],
              usage=None)


def test_boundary_1_usage_only_chunk_has_no_choices():
    usage = NS(model_dump=lambda: {"prompt_tokens": 3, "completion_tokens": 5, "total_tokens": 8})
    chunk = NS(choices=[], usage=usage)
    events = chunk_to_events(chunk, {})
    assert [(e.type, e.data) for e in events] == [
        ("model.usage", {"usage": {"prompt_tokens": 3, "completion_tokens": 5, "total_tokens": 8}})
    ]


def test_boundary_1b_none_choices_chunk_is_ignored():
    assert chunk_to_events(NS(choices=None, usage=None), {}) == []


def test_boundary_2_empty_delta_does_not_emit_event():
    # None(role 元数据)与空串都不产生事件 → 不漂移业务 seq(TC04)
    assert chunk_to_events(text_chunk(None), {}) == []
    assert chunk_to_events(text_chunk(""), {}) == []


def test_tool_call_fragments_accumulate_until_finish():
    pending: dict = {}
    frag1 = NS(choices=[NS(delta=NS(content=None, tool_calls=[
        NS(index=0, id="call_1", function=NS(name="read_file", arguments='{"pa')),
    ]), finish_reason=None)], usage=None)
    frag2 = NS(choices=[NS(delta=NS(content=None, tool_calls=[
        NS(index=0, id=None, function=NS(name=None, arguments='th": "a.py"}')),
    ]), finish_reason="tool_calls")], usage=None)

    events = chunk_to_events(frag1, pending) + chunk_to_events(frag2, pending)
    types = [e.type for e in events]
    assert types == ["model.tool_calls", "model.finished"]
    calls = events[0].data["calls"]
    assert calls == [{"call_id": "call_1", "name": "read_file", "arguments": '{"path": "a.py"}'}]
    assert pending == {}  # 聚合后清空,不影响下一轮


def test_finish_without_tool_calls_yields_finished_only():
    events = chunk_to_events(text_chunk("done", finish="stop"), {})
    assert [e.type for e in events] == ["text.delta", "model.finished"]
    assert events[1].data == {"finish_reason": "stop"}
