"""运行配置:全部可由环境变量覆盖,见 .env.example。"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:  # 本地开发加载 .env;缺失时直接读环境
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover
    pass


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


@dataclass(frozen=True)
class GatewayConfig:
    deepseek_api_key: str = ""
    deepseek_base_url: str = "https://api.deepseek.com"
    deepseek_model: str = "deepseek-v4-flash"
    max_loop_steps: int = 8
    max_retries: int = 2
    heartbeat_seconds: float = 15.0
    cancel_grace_seconds: float = 15.0
    event_preview_chars: int = 2000
    tool_root: Path = Path.cwd()

    @classmethod
    def from_env(cls) -> "GatewayConfig":
        return cls(
            deepseek_api_key=os.getenv("DEEPSEEK_API_KEY", ""),
            deepseek_base_url=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
            deepseek_model=os.getenv("DEEPSEEK_MODEL", "deepseek-v4-flash"),
            max_loop_steps=_int("MAX_LOOP_STEPS", 8),
            max_retries=_int("MAX_RETRIES", 2),
            heartbeat_seconds=_float("HEARTBEAT_SECONDS", 15.0),
            cancel_grace_seconds=_float("CANCEL_GRACE_SECONDS", 15.0),
            event_preview_chars=_int("EVENT_PREVIEW_CHARS", 2000),
            tool_root=Path(os.getenv("TOOL_ROOT", os.getcwd())).resolve(),
        )
