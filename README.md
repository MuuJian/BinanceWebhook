# Binance 合约波动提醒

这个程序会实时监控 Binance U 本位合约价格，并通过 Webhook 发送提醒。

Webhook 请求体是 JSON，内容为触发提醒时的币种和当前价格：

```json
{
  "ticker": "BTCUSDT",
  "price": "65432.1"
}
```

监控币种：

- BTCUSDT
- ETHUSDT
- SOLUSDT

## 设置

程序必须设置 `CALL_WEBHOOK_URL`用于接收提醒。

默认规则是：任意币种在最近 2 分钟内上涨或下跌达到 3% 时提醒一次。随后所有
币种进入 30 秒提醒休眠期；休眠期间仍持续接收并记录价格，但不会发送任何提醒。
30 秒结束后，程序立即使用最新的 2 分钟价格窗口重新判断，满足 3% 就再次提醒。

监控规则可以通过下面三个设置修改：

```env
WINDOW_SECONDS=120
THRESHOLD_PCT=3
COOLDOWN_SECONDS=30
```

- `WINDOW_SECONDS`：观察多少秒，默认 120 秒。
- `THRESHOLD_PCT`：触发提醒的波动百分比，默认 3。
- `COOLDOWN_SECONDS`：任意提醒发出后的全局休眠时间，默认 30 秒。

## 查看是否正常

启动后，日志中应该依次看到：

```text
Worker started
Binance Futures WebSocket connected
First aggTrade received
Binance aggTrade healthy
```

`Binance aggTrade healthy` 每 30 秒出现一次，表示三个币种一直在收到实时行情。

普通运行日志通常是白色。断线、数据停止、自动重连或 Webhook 发送失败会显示红色。
程序断线后会清空旧价格并重新连接，不会使用过期价格发送提醒。

每次触发 CALL 时会先输出一条醒目的红色日志，例如：

```text
ERROR app.webhook: CALL triggered: ticker=BTCUSDT price=65432.1 direction=up movement=3.20%; sending Webhook
```

这条 `ERROR` 仅用于突出显示“已经触发”，不代表发送失败；随后出现
`Webhook delivered` 才表示 Webhook 已成功接收。

## 本地运行

需要 Python 3.11 或更新版本：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

打开 `.env`，填写自己的 Webhook 地址：

```env
CALL_WEBHOOK_URL=https://你的-webhook-地址
```

然后启动：

```bash
python main.py
```

运行回归测试：

```bash
python -m unittest discover -v
```

## 本地测试 Webhook

为了避免测试时发送正式提醒，可以先使用 [webhook.site](https://webhook.site/) 的测试
地址，并在 `.env` 中临时降低提醒门槛：

```env
CALL_WEBHOOK_URL=https://webhook.site/你的测试地址
THRESHOLD_PCT=0.01
WARMUP_SECONDS=10
MIN_POINTS=5
```

启动程序后，在 webhook.site 页面查看收到的提醒。测试完成后，把 Webhook 地址换回
正式地址，并删除后三个测试设置。
