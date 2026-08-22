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

每个币种使用独立的固定基准价。实时价格相对基准价上涨或下跌达到设定百分比时
提醒一次，并立即把触发价格设为新的基准价。基准价在观察时间内保持不变；如果
整个观察时间都没有触发提醒，到期时就使用 Binance 最新价格开始下一轮。
程序会逐笔处理 Binance 成交，价格短暂穿过阈值后马上回落也不会因定时采样而漏报。

任意币种提醒后，所有币种进入 30 秒提醒休眠期；休眠期间仍持续接收价格并维护
基准价，但不会发送提醒。休眠结束后继续与各币种原有的固定基准价比较。

例如基准价为 `20000`，价格到 `20400` 达到 2% 并触发提醒后，新的基准价就是
`20400`。随后即使旧价格 `20000` 仍处于观察范围内也不会重复提醒；只有价格再次
相对 `20400` 波动达到阈值，或者观察时间到期，基准价才会改变。

监控规则可以通过下面三个设置修改：

```env
WINDOW_SECONDS=120
THRESHOLD_PCT=3
COOLDOWN_SECONDS=30
```

- `WINDOW_SECONDS`：固定基准价的最长观察时间，默认 120 秒。
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
```

启动程序后，在 webhook.site 页面查看收到的提醒。测试完成后，把 Webhook 地址换回
正式地址，并删除测试用的 `THRESHOLD_PCT` 设置。
