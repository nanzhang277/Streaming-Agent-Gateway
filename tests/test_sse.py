"""SSE 编解码测试矩阵(p15 TC01–TC06 对应项)。"""
from __future__ import annotations

import codecs
import json

from app.events import RunEvent
from app.sse import encode_heartbeat, encode_sse


def make_event(seq: int, data: dict, type_: str = "text.delta") -> RunEvent:
    return RunEvent(run_id="r1", seq=seq, type=type_, data=data)  # type: ignore[arg-type]


# TC01 中文 Unicode 往返不丢字
def test_tc01_unicode_roundtrip():
    event = make_event(0, {"delta": "正在分析 中文≠ascii ✅"})
    raw = encode_sse(event)
    assert "正在分析".encode("utf-8") in raw
    payload = json.loads(raw.split(b"\ndata: ", 1)[1].rsplit(b"\n\n", 1)[0].decode("utf-8"))
    assert payload["data"]["delta"] == "正在分析 中文≠ascii ✅"


# TC02/TC03 换行、引号、反斜杠不破坏 SSE 帧结构
def test_tc02_tc03_payload_escaping():
    event = make_event(1, {"delta": '第一行\n第二行 "引号" \\反斜杠/'})
    raw = encode_sse(event).decode("utf-8")
    frames = raw.split("\n\n")
    assert len([f for f in frames if f.strip()]) == 1  # 仍是单条事件
    data_line = [line for line in frames[0].split("\n") if line.startswith("data: ")][0]
    payload = json.loads(data_line[len("data: "):])
    assert payload["data"]["delta"] == '第一行\n第二行 "引号" \\反斜杠/'


# TC05 心跳是注释行,精确字节、不占 seq
def test_heartbeat_is_comment():
    assert encode_heartbeat() == b": heartbeat\n\n"


# TC06 拆包/粘包:任意字节切分都能恢复出原事件
class IncrementalSSEParser:
    """模拟浏览器/CLI 侧解析:字节流 buffer → 完整事件。"""

    def __init__(self) -> None:
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self._buf = ""

    def feed(self, chunk: bytes) -> list[dict]:
        self._buf += self._decoder.decode(chunk)
        events = []
        while "\n\n" in self._buf:
            block, self._buf = self._buf.split("\n\n", 1)
            parsed: dict[str, str] = {}
            for line in block.split("\n"):
                if line.startswith(":"):
                    continue
                key, _, value = line.partition(": ")
                if key == "data":
                    parsed["data"] = value
            if "data" in parsed:
                events.append(json.loads(parsed["data"]))
        return events


def test_tc06_arbitrary_byte_splits_recover_events():
    events = [
        make_event(0, {"delta": "你好"}),
        make_event(1, {"delta": "world" * 30}),
        make_event(2, {"delta": ""}, type_="run.started"),
        make_event(3, {"code": "OK"}, type_="run.completed"),
    ]
    raw = b"".join(encode_sse(e) for e in events)

    for cut in range(1, len(raw)):  # 逐一尝试每个切点:半条/多条/中文字节中间
        parser = IncrementalSSEParser()
        got = parser.feed(raw[:cut]) + parser.feed(raw[cut:])
        assert [(e["seq"], e["type"]) for e in got] == [(e.seq, e.type) for e in events], cut


def test_frame_fields_match_contract():
    raw = encode_sse(make_event(17, {"delta": "x"})).decode("utf-8")
    assert raw.startswith("id: 17\nevent: text.delta\ndata: ")
    assert raw.endswith("\n\n")
