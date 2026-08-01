"""Resilient Binance USD-M Futures aggregate-trade stream consumer."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from websockets.asyncio.client import connect

from detector import PriceMovementDetector
from webhook import WebhookDispatcher

logger = logging.getLogger(__name__)

# aggTrade is a "Market" stream in Binance's routed Futures WebSocket API.
BINANCE_FUTURES_WS = "wss://fstream.binance.com/market/stream"
NO_DATA_TIMEOUT_SECONDS = 10
MAX_EVENT_AGE_MS = 10_000
MAX_RECONNECT_DELAY_SECONDS = 60
STABLE_CONNECTION_SECONDS = 60


class StaleMarketData(ConnectionError):
    pass


class BinanceAggTradeMonitor:
    def __init__(
        self,
        *,
        symbols: tuple[str, ...],
        detector: PriceMovementDetector,
        dispatcher: WebhookDispatcher,
    ) -> None:
        self.symbols = symbols
        self.detector = detector
        self.dispatcher = dispatcher
        streams = "/".join(f"{symbol.lower()}@aggTrade" for symbol in symbols)
        self.url = f"{BINANCE_FUTURES_WS}?streams={streams}"

    async def run(self, stop_event: asyncio.Event) -> None:
        failures = 0
        while not stop_event.is_set():
            self.detector.clear_price_history()
            received_data = False
            connected_at: float | None = None
            try:
                logger.info("Connecting to Binance USD-M Futures aggTrade stream")
                async with connect(
                    self.url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=10,
                    open_timeout=15,
                    max_queue=1024,
                ) as websocket:
                    connected_at = time.monotonic()
                    logger.info("Binance WebSocket connected")
                    while not stop_event.is_set():
                        try:
                            raw_message = await asyncio.wait_for(
                                websocket.recv(), timeout=NO_DATA_TIMEOUT_SECONDS
                            )
                        except asyncio.TimeoutError as exc:
                            logger.warning(
                                "No Binance market data received for more than %ss; "
                                "discarding the price window and reconnecting",
                                NO_DATA_TIMEOUT_SECONDS,
                            )
                            raise StaleMarketData("market data idle timeout") from exc

                        self.handle_message(raw_message)
                        if not received_data:
                            received_data = True
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.detector.clear_price_history()
                self._log_connection_error(exc)

            if stop_event.is_set():
                break
            if (
                received_data
                and connected_at is not None
                and time.monotonic() - connected_at >= STABLE_CONNECTION_SECONDS
            ):
                # Only a genuinely stable session resets the backoff. A stream
                # that repeatedly connects, emits one event, and immediately
                # fails must continue backing off exponentially.
                failures = 0
            failures += 1
            delay = min(2 ** (failures - 1), MAX_RECONNECT_DELAY_SECONDS)
            logger.info("Reconnecting to Binance in %ss", delay)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    def handle_message(self, raw_message: str | bytes) -> None:
        try:
            message: dict[str, Any] = json.loads(raw_message)
            data = message.get("data", message)
            if data.get("e") != "aggTrade":
                return
            symbol = str(data["s"]).upper()
            price = float(data["p"])
            event_time = int(data["E"])
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            logger.warning("Ignoring malformed Binance message: %s", exc)
            return

        event_age_ms = int(time.time() * 1000) - event_time
        if event_age_ms > MAX_EVENT_AGE_MS:
            raise StaleMarketData(
                f"received {symbol} event that is {event_age_ms}ms old"
            )

        alert = self.detector.process(
            symbol=symbol,
            price=price,
            event_time=event_time,
        )
        if alert is not None:
            payload = alert.as_payload()
            logger.warning(
                "Price movement alert: %s direction=%s current=%s change=%.4f%%",
                symbol,
                alert.direction,
                price,
                alert.change_percent,
            )
            self.dispatcher.enqueue(payload)

    @staticmethod
    def _log_connection_error(exc: Exception) -> None:
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", None)
        if status in {403, 451}:
            logger.error(
                "Binance WebSocket rejected the connection with HTTP %s. "
                "This commonly indicates a deployment-region restriction. "
                "No alert will be generated from old data; choose a Railway "
                "region where Binance Futures is accessible. Error: %s",
                status,
                exc,
            )
        else:
            logger.error(
                "Binance WebSocket disconnected or failed: %s. Price history "
                "was cleared, so no alert can be generated from stale data.",
                exc,
            )
