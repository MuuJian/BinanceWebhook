# Binance 合约价格异动提醒

监控 BTC, ETH, SOL 大额波动，通过 Webhook 发送提醒

> 本项目只负责监控价格和发送提醒，不会自动下单，也不构成投资建议。

## 提醒规则
默认观察最近 5 分钟内的价格变化。

提醒档位可以在 `.env` 中设置：

```env
ALERT_CHANGE_LEVELS=3,6,9
```

表示：

- 上涨 3% 时提醒
- 上涨 6% 时提醒
- 上涨 9% 时提醒
- 下跌方向也使用相同档位

同一币种、同一方向、同一档位不会一直重复提醒。

每次提醒后会暂停 30 秒，避免短时间内连续发送太多消息。

例如 SOL 在 30 秒内从下跌 3% 继续跌到 10%，程序不会连续提醒 3%、6%、9%，而是在冷却结束后只提醒仍然成立的最高档位。


## 环境变量

先复制配置文件：

```bash
cp .env.example .env
```

然后编辑 `.env`：

```env
WEBHOOK_URL=https://example.com/webhook
WEBHOOK_BODY_FORMAT=json
ALERT_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT
ALERT_WINDOW_SECONDS=300
ALERT_CHANGE_LEVELS=3,6,9
WEBHOOK_TIMEOUT_SECONDS=10
```

各项含义：

| 变量 | 说明 |
|---|---|
| `WEBHOOK_URL` | 接收提醒的地址，必须填写 |
| `WEBHOOK_BODY_FORMAT` | `json` 或 `text` |
| `ALERT_SYMBOLS` | 要监控的币种 |
| `ALERT_WINDOW_SECONDS` | 统计最近多少秒的涨跌 |
| `ALERT_CHANGE_LEVELS` | 提醒档位 |
| `WEBHOOK_TIMEOUT_SECONDS` | Webhook 请求超时时间 |

## 本地运行

建议使用 Python 3.11 或更新版本。

创建虚拟环境：

```bash
python3 -m venv .venv
```

启用虚拟环境：

```bash
source .venv/bin/activate
```

安装依赖：

```bash
pip install -r requirements.txt
```

复制配置文件：

```bash
cp .env.example .env
```

填写好 `.env` 后启动：

```bash
python main.py
```

停止程序：

```text
Ctrl+C
```

## 测试提醒

可以使用 webhook.site 创建一个临时接收地址，并写进 `.env`。

为了快速触发提醒，可以暂时设置：

```env
ALERT_SYMBOLS=SOLUSDT
ALERT_CHANGE_LEVELS=0.01
```

然后运行：

```bash
python main.py
```

## 自动恢复

程序断线后会自动重连。

重新连接时会清空旧的价格记录，避免使用断线前的数据继续计算涨跌幅。

Webhook 暂时发送失败时也会自动重试，不会影响价格监控。

## 数据来源

- Binance USDⓈ-M Futures：实时成交价格
- 用户设置的 Webhook：接收提醒并执行后续操作
