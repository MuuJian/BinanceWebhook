# Binance 合约波动电话提醒 Worker

这是一个独立的 Python 后台 Worker，只负责实时监控 Binance USDⓈ-M Futures 行情，
并把波动提醒发送到 Webhook。固定监听：

- `BTCUSDT`
- `ETHUSDT`
- `SOLUSDT`

项目没有网页、HTTP 服务、域名、`PORT`、Healthcheck、电话或邮件代码，也不需要
Binance API Key。电话由 fwalert 等外部 Webhook 服务负责。

## 行情与内存占用

程序订阅实时 `aggTrade`，不会用 REST 轮询。每一笔成交都会参与计算，但不会把每笔
成交长期保存在内存中：同一秒内的成交会压缩成一个 OHLC 桶，保留该秒的最高价、
最低价、收盘价和成交数。因此默认 120 秒窗口中，每个币种大约只保留 120 个桶；
快速插针的高低点仍然保留。

默认每 30 秒输出一次行情健康日志，包括当前价格、窗口成交数、桶数量和数据年龄。
任一币种超过 10 秒没有新数据，全部价格窗口会立即清空并重连，不会使用过期行情
发送提醒。

## 波动提醒规则

默认使用最近 120 秒窗口：

```text
上涨百分比 = (当前价 / 窗口最低价 - 1) * 100
下跌百分比 = (1 - 当前价 / 窗口最高价) * 100
```

`THRESHOLD_PCT=3` 会自动生成 3%、6%、9%、12%……递进档位：

- 同一轮上涨或下跌，每跨过一个新档位提醒一次；
- 30 秒冷静期内跨档会暂存，冷静期结束时只有仍满足该档才发送；
- 从 9%回落到 7%或 4%不会倒序重复提醒；
- 只有波动回到 3%以内，该方向的档位才复位；
- 上涨和下跌分别记录；
- 若一个宽幅窗口同时满足上涨和下跌，使用发生时间更近的参考极值，避免同一时刻
  发出相反提醒。

提醒是纯文本，例如：

```text
BTCUSDT 合约2分钟内上涨3.24%，当前价格70120.5，参考价格67920
ETHUSDT 合约2分钟内下跌3.12%，当前价格3686，参考价格3800
```

## 配置

项目不使用 YAML。配置只来自 Railway Variables 或本地 `.env`，程序内提供默认值。

本地复制模板：

```bash
cp .env.example .env
```

必须填写：

```env
CALL_WEBHOOK_URL=https://fwalert.com/你的-webhook-id
```

`.env` 已加入 `.gitignore`，不会提交到 Git。旧版使用的 `WEBHOOK_URL` 暂时仍兼容，
但建议改成 `CALL_WEBHOOK_URL`。

可选变量：

| 变量 | 默认值 | 用途 |
|---|---:|---|
| `WINDOW_SECONDS` | `120` | 滚动价格窗口 |
| `THRESHOLD_PCT` | `3` | 基础档位；自动形成 3/6/9% 等档位 |
| `COOLDOWN_SECONDS` | `30` | 每币种、每方向发送间隔 |
| `EVALUATION_INTERVAL_SECONDS` | `1` | 波动评估间隔 |
| `MIN_POINTS` | `20` | 预热所需最少成交数 |
| `WARMUP_SECONDS` | `60` | 预热所需最短窗口跨度 |
| `WEBHOOK_TIMEOUT_SECONDS` | `10` | 单次请求超时 |
| `WEBHOOK_MAX_RETRIES` | `3` | 失败后的最大重试次数 |
| `LOG_LEVEL` | `INFO` | 日志等级 |
| `WS_PROXY_URL` | 空（强制直连） | 明确启用 WebSocket 代理 |

默认连接时显式传入 `proxy=None`，所以 `HTTP_PROXY`、`HTTPS_PROXY` 等系统变量不会
暗中让 Binance WebSocket 走代理。只有设置 `WS_PROXY_URL` 才会启用代理，代理地址
不会输出到日志。Webhook 客户端同样不读取系统代理变量。

## Webhook

请求使用：

```http
Content-Type: text/plain; charset=utf-8
```

Webhook 发送在独立异步队列中，不阻塞行情接收。网络错误、超时、HTTP 429 和 5xx
最多重试 3 次，总尝试最多 4 次，等待 1、2、4 秒；其他 4xx 不重试。日志永远不会
输出完整 Webhook URL。

fwalert 模板建议：

```text
变量名：message
变量来源：请求体 body
提取规则：正则表达式
正则表达式：(.*)
通知简述：{{message}}
通知正文：{{message}}
```

## 断线保护

- WebSocket 使用 ping/pong 心跳检测；
- 任一币种超过 10 秒没有新成交，立即清空全部窗口；
- 超过 10 秒的旧事件或异常时钟事件不会进入窗口；
- Binance 主动断开或连接异常后按 1、2、4、8、16、30 秒退避重连；
- 每次重新连接都从空窗口重新预热，不会根据断线前的旧价格提醒；
- 收到 SIGINT/SIGTERM 时优雅停止，并尽量排空已入队的 Webhook。

## 本地运行

需要 Python 3.11 或更新版本：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python main.py
```

也可以在 VS Code 的“运行和调试”中选择 `Binance Webhook Worker`。按 `Ctrl+C`
会优雅停止，并尽量排空已经入队的 Webhook。

### 本地测试 Webhook

建议把 `CALL_WEBHOOK_URL` 临时改成 [webhook.site](https://webhook.site/) 的接收地址，
再在本地 `.env` 临时设置：

```env
THRESHOLD_PCT=0.01
WARMUP_SECONDS=10
MIN_POINTS=5
```

在 webhook.site 查看收到的纯文本。测试结束后删除这三个临时变量。不要用真实
fwalert 地址做低阈值测试，否则可能频繁触发电话。

## Railway 部署

1. 从 GitHub 仓库创建后台 Worker；
2. 在 Variables 页面设置并 Seal `CALL_WEBHOOK_URL`；
3. Start Command 填写 `python main.py`；
4. 不生成域名，不设置 Networking、`PORT` 或 Healthcheck；
5. 关闭 Serverless/App Sleeping，保证 Worker 持续运行。

普通启动、连接成功、行情健康和发送成功日志写到 stdout，在 Railway 通常显示白色；
断线、数据过期、重连和发送失败写到 stderr，显示红色。若出现 HTTP 403/451，日志会
明确提示部署地区可能无法访问 Binance Futures，并确保不基于旧数据提醒。
