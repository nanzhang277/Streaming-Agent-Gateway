"""Agent Loop 工具循环:模型→工具→模型,tool 事件成对,LOOP_MAX_STEPS 兜底。"""
from __future__ import annotations

from pathlib import Path

from app.adapter.base import ModelStreamEvent
from tests.conftest import collect_events, create_run, make_app, make_client


def tool_call_steps(tmp_path: Path):
    read_call = ModelStreamEvent(
        "model.tool_calls",
        {"calls": [{"call_id": "call_1", "name": "read_file", "arguments": '{"path": "notes.txt"}'}]},
    )
    return [
        [
            ModelStreamEvent("text.delta", {"delta": "我先看看文件。"}),
            read_call,
            ModelStreamEvent("model.finished", {"finish_reason": "tool_calls"}),
        ],
        [
            ModelStreamEvent("text.delta", {"delta": "文件内容是 hello"}),
            ModelStreamEvent("model.finished", {"finish_reason": "stop"}),
            ModelStreamEvent("model.usage", {"usage": {"prompt_tokens": 9, "completion_tokens": 4, "total_tokens": 13}}),
        ],
    ]


async def test_tool_loop_emits_paired_tool_events(tmp_path: Path):
    (tmp_path / "notes.txt").write_text("hello", encoding="utf-8")
    app, _ = make_app(tool_call_steps(tmp_path), tool_root=tmp_path)
    async with make_client(app) as client:
        run_id = await create_run(client)
        events = await collect_events(client, run_id)
        types = [e.type for e in events]

        assert types.count("tool.started") == 1
        assert types.count("tool.completed") == 1
        i_start, i_done = types.index("tool.started"), types.index("tool.completed")
        assert i_done == i_start + 1                      # 成对出现
        assert events[i_start].data["name"] == "read_file"
        assert events[i_done].data["ok"] is True
        assert "hello" in events[i_done].data["result"]

        # 第二轮模型拿到工具结果后给出最终回答
        assert types[-1] == "run.completed"
        final_deltas = "".join(
            e.data["delta"] for e in events if e.type == "text.delta"
        )
        assert "文件内容是 hello" in final_deltas


async def test_unknown_tool_completes_with_error_not_crash(tmp_path: Path):
    call = ModelStreamEvent(
        "model.tool_calls",
        {"calls": [{"call_id": "c9", "name": "no_such_tool", "arguments": "{}"}]},
    )
    steps = [
        [call, ModelStreamEvent("model.finished", {"finish_reason": "tool_calls"})],
        [ModelStreamEvent("text.delta", {"delta": "好的"}),
         ModelStreamEvent("model.finished", {"finish_reason": "stop"})],
    ]
    app, _ = make_app(steps, tool_root=tmp_path)
    async with make_client(app) as client:
        run_id = await create_run(client)
        events = await collect_events(client, run_id)
        done = [e for e in events if e.type == "tool.completed"][0]
        assert done.data["ok"] is False
        assert "未知工具" in done.data["result"]
        assert events[-1].type == "run.completed"         # 工具失败 ≠ Run 失败


async def test_loop_max_steps_guard(tmp_path: Path):
    call = ModelStreamEvent(
        "model.tool_calls",
        {"calls": [{"call_id": "c1", "name": "list_dir", "arguments": "{}"}]},
    )
    steps = [[call, ModelStreamEvent("model.finished", {"finish_reason": "tool_calls"})]]
    app, _ = make_app(steps, tool_root=tmp_path, max_loop_steps=3)
    async with make_client(app) as client:
        run_id = await create_run(client)
        events = await collect_events(client, run_id, max_events=100)
        assert events[-1].type == "run.failed"
        assert events[-1].data["code"] == "LOOP_MAX_STEPS"


async def test_path_escape_rejected(tmp_path: Path):
    call = ModelStreamEvent(
        "model.tool_calls",
        {"calls": [{"call_id": "c2", "name": "read_file", "arguments": '{"path": "../../etc/passwd"}'}]},
    )
    steps = [
        [call, ModelStreamEvent("model.finished", {"finish_reason": "tool_calls"})],
        [ModelStreamEvent("text.delta", {"delta": "越界了"}),
         ModelStreamEvent("model.finished", {"finish_reason": "stop"})],
    ]
    app, _ = make_app(steps, tool_root=tmp_path)
    async with make_client(app) as client:
        run_id = await create_run(client)
        events = await collect_events(client, run_id)
        done = [e for e in events if e.type == "tool.completed"][0]
        assert done.data["ok"] is False
        assert "越界" in done.data["result"]
