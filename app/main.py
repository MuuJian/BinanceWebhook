"""Application lifecycle for the Binance alert background worker."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

from app.alert_engine import Alert, AlertEngine
from app.config import AppConfig, ConfigError, load_config
from app.market_stream import BinanceAggTradeReceiver
from app.price_window import PriceWindowStore
from app.webhook import WebhookWorker

logger = logging.getLogger(__name__)


class _BelowWarning(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < logging.WARNING


def configure_logging(level: int) -> None:
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(level)
    stdout_handler.addFilter(_BelowWarning())
    stdout_handler.setFormatter(formatter)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(max(level, logging.WARNING))
    stderr_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)
    root.addHandler(stdout_handler)
    root.addHandler(stderr_handler)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


async def run_worker(config: AppConfig) -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, name, None)
        if sig is not None:
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:
                pass

    store = PriceWindowStore(config.symbols, config.window_seconds)
    queue: asyncio.Queue[Alert] = asyncio.Queue(maxsize=100)
    receiver = BinanceAggTradeReceiver(
        websocket_url=config.websocket_url,
        websocket_proxy=config.websocket_proxy,
        symbols=config.symbols,
        store=store,
    )
    engine = AlertEngine(
        symbols=config.symbols,
        store=store,
        queue=queue,
        threshold_pct=config.threshold_pct,
        cooldown_seconds=config.cooldown_seconds,
        evaluation_interval_seconds=config.evaluation_interval_seconds,
        min_points=config.min_points,
        warmup_seconds=config.warmup_seconds,
    )
    webhook = WebhookWorker(config.webhook, queue)

    logger.info(
        "Worker started: symbols=%s window=%ss threshold=%g%% "
        "global_cooldown=%gs evaluation=%gs",
        ",".join(config.symbols),
        config.window_seconds,
        config.threshold_pct,
        config.cooldown_seconds,
        config.evaluation_interval_seconds,
    )

    market_task = asyncio.create_task(receiver.run(stop_event), name="market")
    engine_task = asyncio.create_task(engine.run(stop_event), name="engine")
    webhook_task = asyncio.create_task(webhook.run(), name="webhook")
    worker_tasks = {market_task, engine_task, webhook_task}
    stop_task = asyncio.create_task(stop_event.wait(), name="stop-signal")
    done, _ = await asyncio.wait(
        worker_tasks | {stop_task}, return_when=asyncio.FIRST_COMPLETED
    )
    fatal_error: BaseException | None = None
    for task in done:
        if task is stop_task:
            continue
        if task.cancelled():
            fatal_error = RuntimeError(f"{task.get_name()} task was cancelled")
        else:
            fatal_error = task.exception() or RuntimeError(
                f"{task.get_name()} task stopped unexpectedly"
            )
        break

    stop_event.set()
    logger.info("Shutdown requested; stopping market receiver and alert engine")
    await asyncio.gather(market_task, engine_task, return_exceptions=True)
    if fatal_error is None or not webhook_task.done():
        try:
            await asyncio.wait_for(queue.join(), timeout=15)
        except asyncio.TimeoutError:
            logger.warning("Webhook queue did not drain within 15s")
    webhook_task.cancel()
    stop_task.cancel()
    await asyncio.gather(webhook_task, stop_task, return_exceptions=True)
    logger.info("Worker stopped")
    if fatal_error is not None:
        raise RuntimeError("A background task failed") from fatal_error


def main() -> None:
    try:
        config = load_config()
    except ConfigError as exc:
        configure_logging(logging.INFO)
        logger.critical("Configuration error: %s", exc)
        raise SystemExit(2) from exc
    configure_logging(config.log_level)
    try:
        asyncio.run(run_worker(config))
    except KeyboardInterrupt:
        pass
