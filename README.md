# Binance 合约波动提醒

实时监控 Binance U 本位合约的 BTCUSDT、ETHUSDT 和 SOLUSDT，价格达到设定
波动幅度时通过 Webhook 发送提醒。

## 提醒规则

- 每个币种都有独立的固定基准价。
- Binance 每一笔成交都会立即与基准价比较。
- 达到 `THRESHOLD_PCT` 后立即提醒，并把触发价格设为新基准价。
- 触发后没有休眠；下一笔成交再次达到阈值时可以继续提醒。
- 如果 `WINDOW_SECONDS` 内没有触发，到期后用 Binance 最新价格更新基准价。

`WINDOW_SECONDS` 只是基准价最长保持时间，不会建立价格桶，也不会比较窗口内的
最高价和最低价。

## 配置

复制配置文件：

```bash
cp .env.example .env
```

打开 `.env`，至少填写 Webhook 地址：

```env
CALL_WEBHOOK_URL=https://你的-webhook-地址
WINDOW_SECONDS=120
THRESHOLD_PCT=3
```

上面的设置表示：固定基准价最多保持 2 分钟，价格相对基准价上涨或下跌达到 3%
时提醒。其他可选参数和中文说明见 `.env.example`。

## Webhook 内容

```json
{
  "ticker": "BTCUSDT",
  "price": "65432.1"
}
```

`price` 是字符串。

## 运行

需要 Python 3.11 或更新版本：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

启动成功后会看到 `Binance Futures WebSocket connected`。每次触发提醒会先输出
`CALL triggered`；出现 `Webhook delivered` 表示发送成功。

## 测试

```bash
python -m unittest discover -v
```
