"""Application lifecycle for the Binance Futures volatility worker."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

from app.binance_ws import BinanceMiniTickerReceiver
from app.config import AppConfig, ConfigError, load_config
from app.evaluator import ALERT_LOG_LEVEL, Alert, PriceEvaluator
from app.notifier import WebhookSender, WebhookWorker
from app.price_window import PriceWindowStore

logger = logging.getLogger(__name__)
ALERT_QUEUE_SIZE = 100


def configure_logging(level: int) -> None:
    logging.addLevelName(ALERT_LOG_LEVEL, "ALERT")
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
        force=True,
    )
    # httpx otherwise logs the full secret-bearing Webhook URL at INFO level.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:  # pragma: no cover - Windows fallback
            signal.signal(
                sig,
                lambda *_args, event=stop_event: loop.call_soon_threadsafe(
                    event.set
                ),
            )


async def run(config: AppConfig) -> None:
    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)
    store = PriceWindowStore(config.symbols, config.window_seconds)
    alert_queue: asyncio.Queue[Alert] = asyncio.Queue(maxsize=ALERT_QUEUE_SIZE)

    receiver = BinanceMiniTickerReceiver(
        websocket_url=config.websocket_url,
        symbols=config.symbols,
        store=store,
    )
    evaluator = PriceEvaluator(
        store=store,
        alert_queue=alert_queue,
        symbols=config.symbols,
        window_seconds=config.window_seconds,
        threshold_pct=config.threshold_pct,
        cooldown_seconds=config.cooldown_seconds,
        evaluation_interval_seconds=config.evaluation_interval_seconds,
        min_points=config.min_points,
        warmup_seconds=config.warmup_seconds,
    )
    sender = WebhookSender(
        url=config.webhook.url,
        timeout_seconds=config.webhook.timeout_seconds,
        max_retries=config.webhook.max_retries,
    )
    webhook_worker = WebhookWorker(queue=alert_queue, sender=sender)
    await sender.start()

    tasks = {
        asyncio.create_task(receiver.run(stop_event), name="websocket-receiver"),
        asyncio.create_task(evaluator.run(stop_event), name="price-evaluator"),
        asyncio.create_task(webhook_worker.run(), name="webhook-worker"),
    }
    stop_task = asyncio.create_task(stop_event.wait(), name="stop-signal")

    logger.info(
        "Worker started: symbols=%s window=%ss threshold=%.2f%% "
        "cooldown=%ss evaluation=%ss",
        ",".join(config.symbols),
        config.window_seconds,
        config.threshold_pct,
        config.cooldown_seconds,
        config.evaluation_interval_seconds,
    )

    try:
        done, _ = await asyncio.wait(
            tasks | {stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in done:
            if task is not stop_task:
                await task
    finally:
        stop_event.set()
        stop_task.cancel()
        for task in tasks:
            if task.get_name() != "webhook-worker":
                task.cancel()
        await asyncio.gather(
            *(task for task in tasks if task.get_name() != "webhook-worker"),
            stop_task,
            return_exceptions=True,
        )

        pending_messages = alert_queue.qsize() + 1
        retry_delay = 2**config.webhook.max_retries - 1
        per_message_timeout = (
            config.webhook.timeout_seconds * (config.webhook.max_retries + 1)
            + retry_delay
            + 2
        )
        drain_timeout = min(300.0, max(15.0, pending_messages * per_message_timeout))
        try:
            await asyncio.wait_for(alert_queue.join(), timeout=drain_timeout)
        except asyncio.TimeoutError:
            logger.error("Timed out while draining the Webhook queue")

        for task in tasks:
            if task.get_name() == "webhook-worker":
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await sender.close()
        logger.info("Worker stopped cleanly")


def main() -> None:
    try:
        config = load_config()
    except ConfigError as exc:
        configure_logging(logging.INFO)
        logger.critical("Configuration error: %s", exc)
        raise SystemExit(2) from exc

    configure_logging(config.log_level)
    try:
        asyncio.run(run(config))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
