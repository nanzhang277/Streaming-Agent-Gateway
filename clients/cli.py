"""CLI 客户端(p26):Web / CLI / IDE 共享同一套 Harness Event 协议。

用法:
  python clients/cli.py "看看当前目录有什么文件"
  python clients/cli.py --run-id <id>            # 重放历史 Run(审计)
  Ctrl+C                                          # 走 Cancel API 停"做"
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys

import httpx

RESET, DIM = "\x1b[0m", "\x1b[2m"
COLOR = {
    "run.started": "\x1b[36m",
    "text.delta": "\x1b[37m",
    "tool.started": "\x1b[33m",
    "tool.completed": "\x1b[33m",
    "run.retrying": "\x1b[35m",
    "run.completed": "\x1b[32m",
    "run.failed": "\x1b[31m",
    "run.cancelled": "\x1b[33m",
}
TERMINALS = {"run.completed", "run.failed", "run.cancelled"}
STATE = {"run_id": None, "base": "http://127.0.0.1:8000"}


async def consume(client: httpx.AsyncClient, base: str, run_id: str, last: int = -1) -> None:
    cur: dict = {}
    async with client.stream("GET", f"{base}/runs/{run_id}/events",
                             params={"last_event_id": last}) as resp:
        resp.raise_for_status()
        async for line in resp.aiter_lines():
            if line == "":
                if cur:
                    ev = json.loads(cur["data"])
                    typ = ev["type"]
                    if typ == "text.delta":
                        sys.stdout.write(ev["data"].get("delta") or "")
                        sys.stdout.flush()
                    else:
                        color = COLOR.get(typ, "")
                        preview = json.dumps(ev["data"], ensure_ascii=False)
                        print(f"\n{color}[{ev['seq']:>3}] {typ}{RESET} {DIM}{preview[:160]}{RESET}")
                    if typ in TERMINALS:
                        print()
                        return
                    cur = {}
            elif line.startswith(":"):
                continue  # 心跳注释行
            else:
                key, _, value = line.partition(": ")
                if key == "data":
                    cur["data"] = value


async def main() -> None:
    parser = argparse.ArgumentParser(description="Streaming Agent Gateway CLI")
    parser.add_argument("task", nargs="?", help="发给 Agent 的任务")
    parser.add_argument("--base", default=STATE["base"])
    parser.add_argument("--run-id", help="跳过创建,直接重放已有 Run(审计)")
    args = parser.parse_args()
    STATE["base"] = args.base

    timeout = httpx.Timeout(10, read=None)  # SSE 长连接:读不超时
    async with httpx.AsyncClient(timeout=timeout) as client:
        if args.run_id:
            run_id = args.run_id
            print(f"{DIM}审计重放 {run_id}{RESET}")
        else:
            if not args.task:
                parser.error("需要 task(或 --run-id)")
            resp = await client.post(f"{args.base}/runs", json={"task": args.task})
            resp.raise_for_status()
            data = resp.json()
            run_id = data["run_id"]
            STATE["run_id"] = run_id
            print(f"{DIM}run_id={run_id} trace_id={data['trace_id']}{RESET}")
        STATE["run_id"] = run_id
        await consume(client, args.base, run_id)


def entry() -> None:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        run_id = STATE["run_id"]
        if run_id:
            # p27:Ctrl+C 只停了本地读取;真正"停做"必须走 Cancel API
            try:
                httpx.post(f"{STATE['base']}/runs/{run_id}/cancel", timeout=5)
                print(f"\n已请求服务端取消 run {run_id}")
            except Exception as exc:
                print(f"\n取消请求失败:{exc}")


if __name__ == "__main__":
    entry()
