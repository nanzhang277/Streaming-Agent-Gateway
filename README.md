# Streaming Agent Gateway

把一次同步的模型调用,升级为**可观察、可停、可续、可审计**的 Agent 事件流网关。

- **可见** —— `text.delta` / `tool.started` / `run.retrying` 等语义事件实时推送,工具执行期不再是黑盒
- **可停** —— `POST /runs/{id}/cancel` → `cancelling` 过渡态 → 协作式检查 → 宽限期后强制 `task.cancel()`,唯一终态 `run.cancelled`
- **可续** —— SSE `Last-Event-ID` + Run Store 按 seq 重放;断线续传恢复的是业务事件,不是模型 Token,永不重跑/重复计费
- **可审计** —— 每个事件带 `run_id + seq` 留档;任意 run_id 可完整重放事实轨迹,TTFT/重试/工具状态/终态可查

## 架构

```
DeepSeek (ChatCompletionChunk)
  ↓
Model Adapter        chunk → ModelStreamEvent,供应商协议止步于此
  ↓
Agent Loop           生产业务事实(8 类事件),不懂 HTTP
  ↓
Run Store (内存)     单一终态守卫、seq 分配、事件留档;接口可换 PostgreSQL+Redis
  ↓
SSE Gateway          编码 / 心跳 / Last-Event-ID 重放
  ↓
Client               Web 测试台(EventSource)/ CLI(httpx),只解析 RunEvent
```

REST 三接口分离:`POST /runs` 创建(202 立即返回 run_id)· `GET /runs/{id}/events` 订阅 · `POST /runs/{id}/cancel` 控制。**断开订阅 ≠ 取消任务**。

## 快速开始

```bash
pip install -r requirements.txt
copy .env.example .env        # 填入 DEEPSEEK_API_KEY
python -m uvicorn app.main:create_app --factory --host 127.0.0.1 --port 8000
```

浏览器打开 <http://127.0.0.1:8000/> 即是前端测试台;或用 CLI:

```bash
python clients/cli.py "用 list_dir 看看当前目录,然后总结"
python clients/cli.py --run-id <id>     # 重放历史 Run(审计)
```

> 默认模型为 `deepseek-v4-flash`;若你的账号没有该型号,在 `.env` 中把 `DEEPSEEK_MODEL` 改为 `deepseek-chat`。

## 前端测试台:四个能力怎么测

页面右侧是事件时间线(model / tool / control 三色),中间是输出区:

1. **可见** —— 点「① 创建 Run」,观察事件逐条到达与打字机输出;TTFT 记录首个非空 `text.delta`。
2. **可停** —— 流式中点「② 取消」,请求 `cancelling` 后仍保持订阅,直到 `run.cancelled` 终态送达;已产生的部分输出进入存档。
3. **可续** —— 点「③ 断开订阅」,Run 在服务端继续执行(轮询状态可证);点「④ 重连」,从 last seq 补发事件,不重跑模型。
4. **可审计** —— Run 结束后把 run_id 粘进左下输入框点「重放」,完整事实轨迹重放一遍。

## API

```bash
# 创建 Run(202)
curl -X POST localhost:8000/runs -H "Content-Type: application/json" \
     -d '{"task": "用 list_dir 看看当前目录"}'
# => {"run_id": "...", "trace_id": "...", "subscribe": "/runs/{id}/events"}

# 订阅事件流(SSE;重连带 ?last_event_id=N 或 Last-Event-ID 头)
curl -N "localhost:8000/runs/{id}/events?last_event_id=0"

# 状态 / 审计摘要
curl localhost:8000/runs/{id}

# 取消
curl -X POST localhost:8000/runs/{id}/cancel   # 终态后重复取消 → 409
```

事件协议(8 类,`run_id + seq` 单调递增):

```
run.started · text.delta · tool.started · tool.completed
run.retrying · run.completed · run.failed · run.cancelled
```

SSE 帧:`id:` = seq(浏览器重连自动回传 Last-Event-ID),`event:` = 类型,`data:` = 完整 RunEvent JSON;心跳为注释行 `: heartbeat`,不占业务 seq。

## 工具

内置两个只读工具 `list_dir` / `read_file`,路径锁死在 `TOOL_ROOT` 内,用于演示 tool 事件的成对产生与工具失败不拖垮 Run 的语义。接 shell 等有副作用的工具时,必须补进程句柄 + SIGTERM 的取消语义,不可逆操作需幂等 key + 状态查询。

## 实现顺序

RunEvent 协议 → SSE 编解码 → StreamingModel Protocol → DeepSeek Adapter(含 tool_calls 分片聚合)→ Run Store → Agent Loop(重试边界/取消检查)→ 三接口 → Web/CLI 客户端 → 异常测试 → 存档 → TTFT 统计。

## 测试

```bash
pip install -r requirements-dev.txt
python -m pytest -q        # 27 项:SSE 矩阵 / Store / Adapter 边界 / 端到端 / 取消 / 工具循环
```

测试通过 `create_app(model=ScriptedModel(...))` 注入脚本化模型,不依赖 API Key;`ScriptedModel` 只存在于测试,运行时只有 DeepSeekAdapter。

## 边界说明(v1 未含,接口已预留)

- **持久化**:Run Store 为内存实现;替换 PostgreSQL(`UNIQUE(run_id, seq)`)+ Redis 通知时方法签名不变 —— 但进程重启后内存事实丢失,跨进程恢复需先落库。
- **Checkpoint 恢复**:取消/失败时已保存存档(部分文本、last_seq、工具状态),但"从 checkpoint 继续跑"尚未实现。
- **重试**:自动重试仅限"尚未产生业务事件"的 429/5xx/网络错误;流中断后走事件重放而非重新生成,避免产生第二份互相矛盾的输出。
