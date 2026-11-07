"""最小只读工具集(p30 的"可重试/只读"档):路径锁死在 TOOL_ROOT 内。

演示 Agent Loop 的 tool.started / tool.completed 事件;
若要接入 shell 等中等代价工具,必须补进程句柄 + SIGTERM 取消语义。
"""
from __future__ import annotations

import asyncio
from pathlib import Path

from .config import GatewayConfig

TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "list_dir",
            "description": "列出工作区内某个目录的内容(一层)。",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "相对工作区根的目录路径,默认 ."}
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_file",
            "description": "读取工作区内一个文本文件(最多 64KB)。",
            "parameters": {
                "type": "object",
                "properties": {"path": {"type": "string", "description": "相对工作区根的文件路径"}},
                "required": ["path"],
            },
        },
    },
]

MAX_READ_BYTES = 64 * 1024


class ToolError(Exception):
    pass


def _safe_path(raw: str, root: Path) -> Path:
    path = (root / raw).resolve()
    if path != root and root not in path.parents:
        raise ToolError(f"路径越界,只能访问 {root} 内的文件: {raw}")
    return path


async def execute_tool(name: str, args: dict, cfg: GatewayConfig) -> dict:
    """返回 {"ok": bool, "result": str};异常由调用方转成 ok=False。"""
    root = cfg.tool_root
    if name == "list_dir":
        path = _safe_path(str(args.get("path") or "."), root)
        if not path.is_dir():
            raise ToolError(f"不是目录: {args.get('path')}")
        entries = await asyncio.to_thread(lambda: sorted(path.iterdir(), key=lambda p: p.name))
        lines = [
            ("d " if p.is_dir() else "f ") + p.name + ("/" if p.is_dir() else "")
            for p in entries[:200]
        ]
        return {"ok": True, "result": "\n".join(lines) or "(空目录)"}

    if name == "read_file":
        raw = str(args.get("path") or "")
        if not raw:
            raise ToolError("缺少 path 参数")
        path = _safe_path(raw, root)
        if not path.is_file():
            raise ToolError(f"文件不存在: {raw}")
        data = await asyncio.to_thread(path.read_bytes)
        note = ""
        if len(data) > MAX_READ_BYTES:
            data = data[:MAX_READ_BYTES]
            note = f"\n…(截断,原始大小 {len(data)}+ bytes)"
        return {"ok": True, "result": data.decode("utf-8", errors="replace") + note}

    return {"ok": False, "result": f"未知工具: {name}"}
