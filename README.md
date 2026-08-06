# Binance 合约波动电话提醒 Worker

纯后台 Python 服务，仅连接 Binance USDⓈ-M Futures WebSocket，固定监听：

- `BTCUSDT`
- `ETHUSDT`
- `SOLUSDT`

程序使用 `miniTicker` 实时价格，在内存中保存最近 120 秒数据。任一合约在窗口内
相对最低价上涨达到 3%，或相对最高价下跌达到 3%，即把纯文本提醒异步发送给
fwalert Webhook。

项目没有网页、HTTP 服务、域名、`PORT`、Healthcheck、电话或邮件实现。电话由
fwalert 外部服务负责，也不需要 Binance API Key。

## 触发规则

```text
up_pct = (当前价 - 窗口最低价) / 窗口最低价 * 100
down_pct = (当前价 - 窗口最高价) / 窗口最高价 * 100
```

每秒评估一次，触发前必须同时满足：

- 窗口跨度至少 60 秒；
- 窗口内至少有 20 个价格点；
- `up_pct >= 3.0` 或 `down_pct <= -3.0`；
- 相同币种、相同方向不在 30 秒冷静期内。

上涨和下跌分别计算冷静期。例如 `BTCUSDT up` 不影响 `BTCUSDT down`，也不影响
其他币种。只要波动条件持续成立，冷静期结束后可以再次提醒。

提醒正文示例：

```text
BTCUSDT 合约2分钟内上涨3.24%，当前价格70120.50，参考价格67920.00
ETHUSDT 合约2分钟内下跌3.12%，当前价格3686.00，参考价格3800.00
```

## 项目结构

```text
app/
  main.py          程序生命周期与三个后台任务
  config.py        YAML、环境变量读取和校验
  binance_ws.py    miniTicker 接收、心跳、过期保护和重连
  price_window.py  每个币种的内存价格窗口
  evaluator.py     每秒评估与方向冷静期
  notifier.py      纯文本 Webhook、重试和独立 Worker
config.yaml        默认业务参数
main.py            Railway 兼容启动入口
```

## 配置

`config.yaml` 只保留有必要调整的默认业务参数：

```yaml
window_seconds: 120
threshold_pct: 3.0
cooldown_seconds: 30
evaluation_interval_seconds: 1
min_points: 20
warmup_seconds: 60
webhook:
  timeout_seconds: 10
  max_retries: 3
```

固定币种和 Binance `miniTicker` 地址写在程序中，避免部署时误改。

复制私密配置模板：

```bash
cp .env.example .env
```

然后填写：

```env
CALL_WEBHOOK_URL=https://fwalert.com/你的-webhook-id
```

`.env` 已被 Git 忽略。程序不会把完整 Webhook URL 输出到日志。

### 网页环境变量覆盖

部署到 Railway 等平台后，无需终端，也无需修改 `config.yaml`。在服务的 Variables
页面增加下面的变量即可覆盖 YAML 默认值：

| 环境变量 | YAML 默认值 | 用途 |
|---|---:|---|
| `CALL_WEBHOOK_URL` | 必填 | fwalert 私密 Webhook 地址 |
| `WINDOW_SECONDS` | `120` | 滚动窗口秒数 |
| `THRESHOLD_PCT` | `3.0` | 上涨/下跌触发百分比 |
| `COOLDOWN_SECONDS` | `30` | 每个币种、每个方向的冷静期 |
| `EVALUATION_INTERVAL_SECONDS` | `1` | 评估间隔 |
| `MIN_POINTS` | `20` | 最少价格点数 |
| `WARMUP_SECONDS` | `60` | 最短预热跨度 |
| `WEBHOOK_TIMEOUT_SECONDS` | `10` | 单次请求超时 |
| `WEBHOOK_MAX_RETRIES` | `3` | 失败后的最大重试次数 |
| `LOG_LEVEL` | `INFO` | 日志等级 |

环境变量的优先级高于 YAML。修改 Railway Variables 后应用 staged changes，平台会
用新参数重新部署 Worker。

## Webhook 行为

请求永远使用纯文本，不发送 JSON：

```http
Content-Type: text/plain; charset=utf-8
```

失败后最多重试 3 次，总尝试次数最多 4 次，间隔为 1、2、4 秒。HTTP 429、5xx、
网络错误和超时会重试；其他 4xx 直接失败。发送运行在独立队列 Worker 中，不会
阻塞 Binance 行情接收。

fwalert 页面设置：

```text
变量名：message
变量来源：请求体 body
提取规则：正则表达式
正则表达式：(.*)
通知简述：{{message}}
通知正文：{{message}}
```

## 行情安全保护

- WebSocket ping/pong 心跳；
- 任一币种超过 10 秒没有当前数据时清空窗口并重连；
- 超过 10 秒的 Binance 旧事件拒绝处理；
- 每次断线都清空全部窗口并重新预热；
- 重连退避为 1、2、4、8、16、30 秒，之后保持 30 秒；
- SIGINT/SIGTERM 优雅停止并尽量排空 Webhook 队列。

因此 Binance 地区限制、代理错误或断线期间不会发送基于旧价格的提醒。

## 本地运行

需要 Python 3.11 或更新版本：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python main.py
```

按 `Ctrl+C` 优雅停止。VS Code 可在“运行和调试”中选择
`Binance Webhook Worker`。

## 本地测试 Webhook

可把 `CALL_WEBHOOK_URL` 临时换成 [webhook.site](https://webhook.site/) 提供的接收
地址，再通过环境变量临时降低触发门槛：

```env
THRESHOLD_PCT=0.01
WARMUP_SECONDS=10
MIN_POINTS=5
```

启动后在 webhook.site 检查纯文本正文。测试完成后删除这些覆盖变量，恢复 YAML
默认值。不要使用真实 fwalert 地址做高频测试，否则可能触发电话。

## Railway 后台 Worker

1. 从 GitHub 仓库部署服务；
2. 在 Variables 页面设置并 Seal `CALL_WEBHOOK_URL`；
3. Start Command 设置为 `python main.py`；
4. 不生成域名；
5. 不设置 Networking、`PORT` 或 Healthcheck；
6. 关闭 Serverless/App Sleeping，保证 Worker 持续运行。

如果 Binance WebSocket 返回 HTTP 403/451，请选择允许访问 Binance Futures 且符合
当地法规及平台条款的部署地区。
