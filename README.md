# Binance Futures Price Movement Webhook Worker

一个独立的 Python 后台 Worker。它只通过 Binance USDⓈ-M Futures WebSocket
`wss://fstream.binance.com/market/stream` 路由
订阅 `aggTrade` 实时成交数据，监控各币种最近 5 分钟的涨跌幅，并异步发送
Webhook。项目不包含网页、HTTP 服务、域名、端口、电话或邮件功能，也不需要
Binance API Key。

## 告警逻辑

默认订阅 `BTCUSDT`、`ETHUSDT`、`SOLUSDT`。每个币种维护独立的滚动价格窗口：

上涨按当前价相对最近 5 分钟最低价计算，下跌按当前价相对最近 5 分钟最高价
计算。告警档位直接通过 `ALERT_CHANGE_LEVELS` 设置，例如 `3,6,9` 表示上涨和
下跌方向分别在 3%、6%、9% 各提醒一次。同一方向、同一档位内不会重复；该方向
恢复到第一档以内后重置。

每个币种发送提醒后固定休息 30 秒，这个时间写死在代码中，不通过环境变量修改。
休息期间继续监控；如果又跨过多个档位，只保留仍然成立的最高档位，休息结束后
再提醒一次，避免短时间连续拨打电话。

断线、超过 10 秒没有行情、收到超过 10 秒的旧事件时，Worker 会清空全部价格
窗口和分级状态并重连。因此重连后不会用旧高点产生错误提醒。

Webhook JSON 示例：

```json
{
  "event": "price_change_alert",
  "market": "BINANCE:BTCUSDT",
  "alert name": "",
  "message": "BTCUSDT 5分钟内下跌3%",
  "direction": "down",
  "alertLevelPercent": 3.0,
  "symbol": "BTCUSDT",
  "currentPrice": 97000.0,
  "peakPrice": 100000.0,
  "lowestPrice": 97000.0,
  "changePercent": -3.0,
  "dropPercent": -3.0,
  "windowSeconds": 300,
  "eventTime": 1785571200000,
  "triggeredAt": "2026-08-01T08:00:00+00:00"
}
```

`market` 是 TradingView `{{exchange}}:{{ticker}}` 占位格式在本项目中的等价值；
由于行情直接来自 Binance，而不是 TradingView，Worker 会直接写入实际值。
`alert name` 和 `message` 是 fwalert 可直接提取的顶层 JSON 字符串字段，分别
对应其通知模板中的 `{{alert name}}` 与 `{{message}}`。所有内容均由 Worker
写入实际值，不包含未解析的 TradingView 占位符。
`eventTime` 是 Binance 提供的毫秒 Unix 时间戳，`triggeredAt` 是 Worker 生成的
UTC ISO 8601 时间。

## 本地运行

建议使用 Python 3.11 或更新版本：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

编辑 `.env`，将 `WEBHOOK_URL` 改为真实接收地址，然后启动：

```bash
python main.py
```

按 `Ctrl+C` 可优雅停止。`.env` 已被 `.gitignore` 忽略，请勿把真实 Webhook
地址写入代码、README、提交记录或 Railway 配置以外的位置。

### VS Code 启动

用 VS Code 打开项目目录，安装工作区推荐的 Python 扩展后，进入“运行和调试”：

- 选择 `Binance Webhook Worker` 可使用 `.venv` 和 `.env` 启动或调试 Worker。

## 环境变量

| 变量 | 默认/示例值 | 说明 |
|---|---:|---|
| `WEBHOOK_URL` | 必填 | 接收 Webhook POST 的 HTTP(S) 地址 |
| `WEBHOOK_BODY_FORMAT` | `json` | `json` 或兼容旧 TradingView/fwalert 规则的 `text` |
| `ALERT_SYMBOLS` | `BTCUSDT,ETHUSDT,SOLUSDT` | 逗号分隔的合约代码 |
| `ALERT_WINDOW_SECONDS` | `300` | 价格滚动窗口 |
| `ALERT_CHANGE_LEVELS` | `3,6,9` | 直接设置上涨、下跌提醒档位，必须递增 |
| `WEBHOOK_TIMEOUT_SECONDS` | `10` | 单次 Webhook 请求超时 |

Webhook 发送最多尝试 3 次，失败后的等待时间按 1 秒、2 秒指数增长。发送由两个
独立异步 Worker 处理，不会阻塞 WebSocket 行情接收。

默认 `json` 模式发送上方完整 JSON。若复用原先接收 TradingView 普通文本消息的
fwalert 链接，请设置 `WEBHOOK_BODY_FORMAT=text`。此时请求使用
`text/plain; charset=utf-8`，正文为：

```text
SOLUSDT 5分钟内下跌3%
```

这样可避免旧 fwalert 规则把 JSON 请求体显示为 `[object Object]`。

项目包含 SOCKS 代理支持；如果本机或部署环境通过 `ALL_PROXY`、`HTTPS_PROXY`
等变量为 WebSocket 配置 SOCKS 代理，Worker 会自动使用该代理。

## 测试 Webhook

1. 在 [webhook.site](https://webhook.site/) 创建临时接收地址（注意不要向第三方
   接收器发送敏感数据），填入本地 `.env`。
2. 先直接验证 Webhook 传输链路：

   ```bash
   python -c 'import asyncio; from config import load_config; from webhook import WebhookSender; c=load_config(); print(asyncio.run(WebhookSender(url=c.webhook_url, timeout_seconds=c.webhook_timeout_seconds, body_format=c.webhook_body_format).send({"alert name":"BTCUSDT 5分钟内下跌3%","message":"BTCUSDT 5分钟内下跌3%","event":"manual_test","symbol":"BTCUSDT"})))'
   ```

3. 再运行 `python main.py` 验证 Binance 实时连接。为了更容易在短时间内看到真实
   行情告警，可在**仅限本地测试**的 `.env` 中临时设置
   `ALERT_CHANGE_LEVELS=0.01,3,6`；测试后恢复为需要的正式档位。

## Railway 部署（后台 Worker）

1. 将项目推送到私有或公开 Git 仓库，并在 Railway 创建服务。
2. 在 Railway Variables 中配置上述环境变量，尤其是 `WEBHOOK_URL`。
3. Start Command 设置为：

   ```text
   python main.py
   ```

4. 这是后台 Worker：**不要生成域名，不要配置 Networking、`PORT` 或
   Healthcheck**。
5. 关闭 Serverless/App Sleeping，确保 Worker 不因无 HTTP 流量而休眠。

某些部署地区无法访问 Binance Futures。若 WebSocket 握手返回 HTTP 403/451，
日志会明确提示可能存在地区限制。请选择 Binance Futures 可访问且符合当地法规
及平台条款的 Railway 部署地区。任何断线都会清空价格窗口，程序不会基于过期
数据发送告警。
