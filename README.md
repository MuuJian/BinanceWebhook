# Binance 合约波动提醒

这个程序会实时监控 Binance U 本位合约价格，并通过 Webhook 发送提醒。

监控币种：

- BTCUSDT
- ETHUSDT
- SOLUSDT

不需要 Binance API Key。

## 提醒规则

默认观察最近 2 分钟的价格，涨跌达到 3%时提醒。

提醒会按 3%、6%、9%、12%……逐级触发。例如已经提醒过 3%，继续上涨到 6%时会再
提醒一次。价格回落时不会倒着重复提醒；只有波动回到 3%以内，才会开始新一轮计算。

同一个币种、同一个方向的两次提醒至少间隔 30 秒。上涨和下跌分开计算。

提醒示例：

```text
BTCUSDT 合约2分钟内上涨3.24%，当前价格70120.5，参考价格67920
ETHUSDT 合约2分钟内下跌3.12%，当前价格3686，参考价格3800
```

## 设置

程序必须设置 `CALL_WEBHOOK_URL`，用于接收提醒。不要把真实地址写进代码或上传到
GitHub。

监控规则可以通过下面三个设置修改：

```env
WINDOW_SECONDS=120
THRESHOLD_PCT=3
COOLDOWN_SECONDS=30
```

- `WINDOW_SECONDS`：观察多少秒，默认 120 秒。
- `THRESHOLD_PCT`：每一档波动百分比，默认 3，即 3%、6%、9%……
- `COOLDOWN_SECONDS`：同币种同方向两次提醒的最短间隔，默认 30 秒。

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

按 `Ctrl+C` 停止。也可以在 VS Code 的“运行和调试”中选择
`Binance Webhook Worker`。

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
