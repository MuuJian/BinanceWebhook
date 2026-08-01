"""Entrypoint for the Binance Futures price alert worker."""

from __future__ import annotations

import asyncio
import logging
import signal

from binance_monitor import BinanceAggTradeMonitor
from config import ConfigError, load_config
from detector import COOLDOWN_SECONDS, PriceMovementDetector
from webhook import WebhookDispatcher, WebhookSender


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


async def run() -> None:
    config = load_config()
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:  # pragma: no cover - Windows fallback
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop_event.set))

    detector = PriceMovementDetector(
        symbols=config.alert_symbols,
        window_seconds=config.alert_window_seconds,
        change_levels=config.alert_change_levels,
    )
    sender = WebhookSender(
        url=config.webhook_url,
        timeout_seconds=config.webhook_timeout_seconds,
        body_format=config.webhook_body_format,
    )
    dispatcher = WebhookDispatcher(sender)
    monitor = BinanceAggTradeMonitor(
        symbols=config.alert_symbols,
        detector=detector,
        dispatcher=dispatcher,
    )

    await dispatcher.start()
    monitor_task = asyncio.create_task(monitor.run(stop_event), name="binance-monitor")
    stop_task = asyncio.create_task(stop_event.wait(), name="stop-signal")

    logging.getLogger(__name__).info(
        "Worker started for %s (window=%ss, change levels=%s%%, cooldown=%ss)",
        ",".join(config.alert_symbols),
        config.alert_window_seconds,
        ",".join(f"{level:g}" for level in config.alert_change_levels),
        COOLDOWN_SECONDS,
    )

    try:
        done, _ = await asyncio.wait(
            {monitor_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if monitor_task in done:
            await monitor_task
    finally:
        stop_event.set()
        stop_task.cancel()
        if not monitor_task.done():
            monitor_task.cancel()
        await asyncio.gather(monitor_task, stop_task, return_exceptions=True)
        await dispatcher.stop()
        logging.getLogger(__name__).info("Worker stopped cleanly")


def main() -> None:
    configure_logging()
    try:
        asyncio.run(run())
    except ConfigError as exc:
        logging.getLogger(__name__).critical("Configuration error: %s", exc)
        raise SystemExit(2) from exc
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
