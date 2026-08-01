# Binance Futures Price Movement Webhook Worker

独立的 Python 后台 Worker，通过 Binance USDⓈ-M Futures WebSocket 实时订阅
`aggTrade`，监控币种在滚动窗口内的上涨和下跌幅度，并异步发送 Webhook。

项目不包含网页、HTTP 服务、域名、`PORT`、Healthcheck、电话或邮件代码，也不需要
Binance API Key。若 `WEBHOOK_URL` 指向电话告警服务，电话行为由外部服务执行。

## 监控逻辑

默认监控 `BTCUSDT`、`ETHUSDT`、`SOLUSDT`，默认窗口为最近 5 分钟：

```text
rise_percent = (current_price / lowest_price - 1) * 100
drop_percent = (current_price / highest_price - 1) * 100
```

提醒档位由 `ALERT_CHANGE_LEVELS` 直接设置。比如：

```env
ALERT_CHANGE_LEVELS=3,6,9
```

代表上涨和下跌方向分别在 3%、6%、9% 提醒。同一方向、同一档位不会重复提醒；
该方向恢复到第一档以内后，才开始新一轮。

每个币种提醒后固定休息 30 秒，这个值写死在代码中。休息期间仍持续监控：如果
跨过更高档位，只保留当前仍成立的最高档，休息结束后再提醒一次。

例如 5 分钟内逐步下跌 10%：

- 跌至 3% 时提醒；
- 30 秒内继续跌至 10%，不会连续拨打；
- 休息结束时若跌幅仍成立，提醒 9% 档。

如果一笔成交直接从未触发状态跳到 10%，只提醒最高已跨越的 9% 档。

提醒文字保持简短：

```text
SOLUSDT 5分钟内上涨3%
SOLUSDT 5分钟内下跌6%
```

## 行情连接与过期数据保护

使用 Binance 当前的 Market 路由：

```text
wss://fstream.binance.com/market/stream
```

- 原生 WebSocket ping/pong 心跳；
- 超过 10 秒没有有效 `aggTrade` 时记录警告并重连；
- 断线自动指数退避，最长 60 秒；
- Binance 主动断开、代理故障和程序异常后自动恢复；
- 收到超过 10 秒的旧事件时拒绝处理；
- 任何断线都会清空价格窗口和待提醒档位，不使用过期高低点；
- SIGINT/SIGTERM 优雅停止。

项目包含 SOCKS 代理支持。如果运行环境设置了 SOCKS 代理，WebSocket 会自动使用。

## Webhook

Webhook 通过独立异步队列发送，不阻塞行情接收：

- 每次请求有超时；
- 最多尝试 3 次；
- 失败后按 1 秒、2 秒指数退避；
- HTTP 408、429、5xx 和网络错误会重试；
- 其他 4xx 直接失败；
- 日志不会输出可能包含令牌的完整 Webhook URL。

### JSON 模式

`WEBHOOK_BODY_FORMAT=json` 时发送完整 JSON：

```json
{
  "event": "price_change_alert",
  "market": "BINANCE:SOLUSDT",
  "alert name": "SOLUSDT 5分钟内下跌3%",
  "message": "SOLUSDT 5分钟内下跌3%",
  "direction": "down",
  "alertLevelPercent": 3.0,
  "symbol": "SOLUSDT",
  "currentPrice": 97.0,
  "referencePrice": 100.0,
  "peakPrice": 100.0,
  "lowestPrice": 97.0,
  "changePercent": -3.0,
  "dropPercent": -3.0,
  "risePercent": 0.0,
  "windowSeconds": 300,
  "eventTime": 1785571200000,
  "triggeredAt": "2026-08-01T08:00:00+00:00"
}
```

### TradingView/fwalert 纯文本兼容模式

复用原先接收 TradingView 普通文本消息的 fwalert 链接时设置：

```env
WEBHOOK_BODY_FORMAT=text
```

请求使用 `Content-Type: text/plain; charset=utf-8`，正文只有一句提醒，避免 fwalert
显示 `[object Object]`。

## 环境变量

复制模板：

```bash
cp .env.example .env
```

| 变量 | 默认/示例值 | 说明 |
|---|---:|---|
| `WEBHOOK_URL` | 必填 | 接收 Webhook POST 的 HTTP(S) 地址 |
| `WEBHOOK_BODY_FORMAT` | `json` | `json` 或 `text` |
| `ALERT_SYMBOLS` | `BTCUSDT,ETHUSDT,SOLUSDT` | 逗号分隔的合约代码 |
| `ALERT_WINDOW_SECONDS` | `300` | 滚动窗口秒数 |
| `ALERT_CHANGE_LEVELS` | `3,6,9` | 递增的上涨/下跌提醒档位 |
| `WEBHOOK_TIMEOUT_SECONDS` | `10` | 单次 Webhook 请求超时 |

真实 Webhook 地址只写入 `.env` 或 Railway Variables。`.env` 已加入 `.gitignore`，
不要把地址写进代码、`.env.example`、README 或提交记录。

## 本地运行

建议使用 Python 3.11 或更新版本：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python main.py
```

按 `Ctrl+C` 停止。

### VS Code

用 VS Code 打开项目，安装推荐的 Python 扩展，在“运行和调试”中选择
`Binance Webhook Worker`。启动配置自动使用 `.venv` 和 `.env`。

### 本地测试 Webhook

可在 [webhook.site](https://webhook.site/) 创建临时接收地址并写入 `.env`。为方便
快速触发，可临时设置：

```env
ALERT_SYMBOLS=SOLUSDT
ALERT_CHANGE_LEVELS=0.01
```

启动 `python main.py`，在接收页面核对正文或 JSON。测试完成后恢复正式档位。
如果 URL 指向 fwalert，测试会触发其外部电话规则。

## Railway 后台 Worker

1. 在 Railway Variables 中配置上述环境变量；
2. Start Command 设置为 `python main.py`；
3. 不生成域名；
4. 不配置 Networking、`PORT` 或 Healthcheck；
5. 关闭 Serverless/App Sleeping，保证 Worker 持续运行。

若 WebSocket 握手返回 HTTP 403/451，日志会提示可能存在部署地区限制。请选择
Binance Futures 可访问且符合当地法规与平台条款的 Railway 地区。
