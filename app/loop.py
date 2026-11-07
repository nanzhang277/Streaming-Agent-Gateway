"""Agent Loop(p20/p41):生产业务事实,不懂 HTTP。

三条边界:
  ① 模型事件 ≠ Run 事件 —— text.delta 直接透传为 Run 事件,
     model.finished/model.usage 由 Loop 消费后裁决;
  ② 流结束 ≠ 任务成功 —— finish_reason=length/content_filter 视为失败;
  ③ CancelledError 是控制信号,绝不许被 except Exception 吞掉。

重试边界(p33/p35):尚未产生业务事件时才允许有限重试
(白名单 429/5xx/网络);已产生业务事件后中断 → run.failed,
恢复走事件重放,禁止重新调用模型。
"""
from __future__ import annotations

import asyncio
import json
import random
import time
from dataclasses import dataclass, field

import openai

from .config import GatewayConfig
from .events import RunCancelled, RunFailed
from .store.base import Run
from .store.memory import InMemoryRunStore
from .tools import TOOL_SCHEMAS, execute_tool

SYSTEM_PROMPT = (
    "你是运行在 Streaming Agent Gateway 中的 Agent。"
    "需要查看文件时调用 list_dir / read_file 工具;回答保持简洁。"
)

# 白名单(p35):DNS/网络/429/5xx
_RETRYABLE = (openai.RateLimitError, openai.InternalServerError, openai.APIConnectionError)


def _fail_code(exc: Exception) -> tuple[str, bool]:
    if isinstance(exc, openai.RateLimitError):
        return "MODEL_RATE_LIMIT", True
    if isinstance(exc, openai.InternalServerError):
        return "MODEL_INTERNAL", True
    if isinstance(exc, openai.APITimeoutError):
        return "MODEL_TIMEOUT", True
    if isinstance(exc, openai.APIConnectionError):
        return "MODEL_CONNECTION", True
    if isinstance(exc, openai.AuthenticationError):
        return "MODEL_AUTH", False
    if isinstance(exc, openai.BadRequestError):
        return "MODEL_BAD_REQUEST", False
    if isinstance(exc, openai.APIStatusError):
        return f"MODEL_HTTP_{getattr(exc, 'status_code', '?')}", False
    return "MODEL_ERROR", False


@dataclass
class StepOutcome:
    text: str = ""
    finish_reason: str | None = None
    tool_calls: list[dict] = field(default_factory=list)
    usage: dict | None = None


class AgentLoop:
    """每次 Run 实例化一个;business_emitted 是重试边界的裁决依据。"""

    def __init__(self, store: InMemoryRunStore, model, cfg: GatewayConfig) -> None:
        self.store = store
        self.model = model
        self.cfg = cfg
        self.business_emitted = False
        self.partial_text = ""
        self.tool_state: list[dict] = []

    async def run(self, run_id: str, task: str) -> None:
        run = self.store.get(run_id)
        assert run is not None
        self.store.mark_running(run_id)
        await self.store.publish(
            run_id, "run.started", {"task": task, "model": getattr(self.model, "model", "unknown")}
        )
        messages: list[dict] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": task},
        ]
        usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        steps = 0
        try:
            for _ in range(self.cfg.max_loop_steps):
                self._check_cancel(run)
                steps += 1
                outcome = await self._model_step(run, run_id, messages)
                for key in usage:
                    usage[key] += int((outcome.usage or {}).get(key) or 0)
                if not outcome.tool_calls:
                    break
                messages.append(
                    {
                        "role": "assistant",
                        "content": outcome.text or None,
                        "tool_calls": [
                            {
                                "id": c["call_id"],
                                "type": "function",
                                "function": {"name": c["name"], "arguments": c["arguments"]},
                            }
                            for c in outcome.tool_calls
                        ],
                    }
                )
                for call in outcome.tool_calls:
                    self._check_cancel(run)
                    await self._run_tool(run, run_id, call, messages)
            else:
                raise RunFailed(
                    "LOOP_MAX_STEPS",
                    f"超过最大循环步数 {self.cfg.max_loop_steps}",
                    retryable=False,
                )
            await self.store.publish(
                run_id,
                "run.completed",
                {
                    "ttft_ms": run.ttft_ms,
                    "duration_ms": self._elapsed_ms(run),
                    "usage": usage,
                    "steps": steps,
                    "tool_calls": len(self.tool_state),
                },
            )
        except asyncio.CancelledError:
            await self._terminate_cancelled(run, run_id, usage, reason="forced_task_cancel")
            raise
        except RunCancelled:
            await self._terminate_cancelled(run, run_id, usage, reason="client_cancel")
        except RunFailed as exc:
            self._archive(run)
            await self.store.publish(
                run_id,
                "run.failed",
                {
                    "code": exc.code,
                    "message": exc.message,
                    "retryable": exc.retryable,
                    "hint": exc.hint,
                },
            )
        except Exception as exc:  # 兜底:任何未知异常都要落一个终态
            self._archive(run)
            await self.store.publish(
                run_id,
                "run.failed",
                {
                    "code": "INTERNAL_ERROR",
                    "message": f"{type(exc).__name__}: {exc}",
                    "retryable": False,
                    "hint": "",
                },
            )

    # ---- 模型步:含重试边界 ----

    async def _model_step(self, run: Run, run_id: str, messages: list[dict]) -> StepOutcome:
        attempt = 0
        while True:
            attempt += 1
            outcome = StepOutcome()
            try:
                async for mev in self.model.stream(messages, TOOL_SCHEMAS):
                    self._check_cancel(run)
                    if mev.type == "text.delta":
                        delta = mev.data.get("delta") or ""
                        if not delta:
                            continue
                        if not self.business_emitted:
                            self.business_emitted = True
                            if run.ttft_ms is None and run.started_perf is not None:
                                run.ttft_ms = round((time.monotonic() - run.started_perf) * 1000, 1)
                        outcome.text += delta
                        self.partial_text += delta
                        await self.store.publish(run_id, "text.delta", {"delta": delta})
                    elif mev.type == "model.tool_calls":
                        outcome.tool_calls = mev.data.get("calls") or []
                    elif mev.type == "model.finished":
                        outcome.finish_reason = mev.data.get("finish_reason")
                    elif mev.type == "model.usage":
                        outcome.usage = mev.data.get("usage")

                if outcome.finish_reason is None:
                    raise RunFailed("MODEL_STREAM_ENDED", "供应商流在 finish_reason 之前结束")
                if outcome.finish_reason == "length":
                    raise RunFailed(
                        "OUTPUT_TRUNCATED",
                        "输出被 max_tokens 截断",
                        hint="结果不完整,不能作为最终答案",
                    )
                if outcome.finish_reason == "content_filter":
                    raise RunFailed("CONTENT_FILTER", "内容被安全策略拦截")

                calls = []
                for i, c in enumerate(outcome.tool_calls):
                    raw_args = c.get("arguments") or "{}"
                    try:
                        parsed = json.loads(raw_args)
                        if not isinstance(parsed, dict):
                            parsed = {"_raw": raw_args}
                    except json.JSONDecodeError:
                        parsed = {"_raw": raw_args}
                    calls.append(
                        {
                            "call_id": c.get("call_id") or f"call_{i}",
                            "name": c.get("name") or "",
                            "arguments": raw_args,
                            "parsed": parsed,
                        }
                    )
                outcome.tool_calls = calls
                return outcome

            except RunFailed:
                raise
            except _RETRYABLE as exc:
                code, _ = _fail_code(exc)
                if self.business_emitted:
                    raise RunFailed(
                        "MODEL_STREAM_INTERRUPTED",
                        f"{type(exc).__name__}: 已产生业务事件后流中断",
                        hint="禁止盲目重新生成(会产生第二份互相矛盾的输出),恢复走事件重放",
                    )
                if attempt > self.cfg.max_retries:
                    raise RunFailed(code, f"{type(exc).__name__}: {exc}", hint="重试次数用尽")
                await self.store.publish(
                    run_id,
                    "run.retrying",
                    {"attempt": attempt, "reason": type(exc).__name__, "message": str(exc)[:200]},
                )
                delay = min(0.5 * 2 ** (attempt - 1), 4.0) + random.uniform(0, 0.25)
                await asyncio.sleep(delay)
            except openai.APIStatusError as exc:
                code, retryable = _fail_code(exc)
                raise RunFailed(code, str(exc)[:300], retryable=retryable)

    # ---- 工具步 ----

    async def _run_tool(self, run: Run, run_id: str, call: dict, messages: list[dict]) -> None:
        call_id, name = call["call_id"], call["name"]
        entry = {"call_id": call_id, "name": name, "status": "running"}
        self.tool_state.append(entry)
        self.business_emitted = True
        await self.store.publish(
            run_id, "tool.started", {"call_id": call_id, "name": name, "arguments": call["parsed"]}
        )
        try:
            result = await execute_tool(name, call["parsed"], self.cfg)
        except Exception as exc:
            result = {"ok": False, "result": f"{type(exc).__name__}: {exc}"}
        entry["status"] = "ok" if result["ok"] else "error"
        await self.store.publish(
            run_id,
            "tool.completed",
            {
                "call_id": call_id,
                "ok": result["ok"],
                "result": result["result"][: self.cfg.event_preview_chars],
            },
        )
        messages.append(
            {"role": "tool", "tool_call_id": call_id, "content": result["result"][:20000]}
        )

    # ---- 收尾 ----

    def _check_cancel(self, run: Run) -> None:
        if run.cancel_event.is_set():
            raise RunCancelled()

    def _archive(self, run: Run) -> None:
        """p32:取消即存档点 —— 保存部分输出与最后 seq,但不等于可执行点。"""
        run.archive = {
            "partial_text": self.partial_text[-800:],
            "last_seq": run.seq,
            "tool_state": self.tool_state,
        }

    async def _terminate_cancelled(
        self, run: Run, run_id: str, usage: dict, *, reason: str
    ) -> None:
        self._archive(run)
        await self.store.publish(
            run_id,
            "run.cancelled",
            {
                "cancelled_at": _utc_now(),
                "reason": reason,
                "last_seq": run.seq,
                "usage": usage,
            },
        )

    @staticmethod
    def _elapsed_ms(run: Run) -> float | None:
        if run.started_perf is None:
            return None
        return round((time.monotonic() - run.started_perf) * 1000, 1)


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()
