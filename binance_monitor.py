"""Resilient Binance USD-M Futures aggregate-trade stream consumer."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from typing import Any

from websockets.asyncio.client import connect

from detector import PriceMovementDetector
from webhook import WebhookDispatcher

logger = logging.getLogger(__name__)

# aggTrade belongs to Binance's routed "Market" Futures WebSocket endpoint.
BINANCE_FUTURES_WS = "wss://fstream.binance.com/market/stream"
NO_DATA_TIMEOUT_SECONDS = 10
MAX_EVENT_AGE_MS = 10_000
MAX_RECONNECT_DELAY_SECONDS = 60
STABLE_CONNECTION_SECONDS = 60


class StaleMarketData(ConnectionError):
    """Raised when the stream cannot provide current market data."""


class BinanceAggTradeMonitor:
    def __init__(
        self,
        *,
        symbols: tuple[str, ...],
        detector: PriceMovementDetector,
        dispatcher: WebhookDispatcher,
    ) -> None:
        self.symbols = symbols
        self._symbol_set = frozenset(symbols)
        self.detector = detector
        self.dispatcher = dispatcher
        streams = "/".join(f"{symbol.lower()}@aggTrade" for symbol in symbols)
        self.url = f"{BINANCE_FUTURES_WS}?streams={streams}"

    async def run(self, stop_event: asyncio.Event) -> None:
        failures = 0
        while not stop_event.is_set():
            self.detector.clear_price_history()
            connected_at: float | None = None
            received_valid_data = False

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
                    data_deadline = connected_at + NO_DATA_TIMEOUT_SECONDS
                    logger.info("Binance WebSocket connected")

                    while not stop_event.is_set():
                        remaining = data_deadline - time.monotonic()
                        if remaining <= 0:
                            raise StaleMarketData("market data idle timeout")
                        try:
                            raw_message = await asyncio.wait_for(
                                websocket.recv(), timeout=remaining
                            )
                        except asyncio.TimeoutError as exc:
                            raise StaleMarketData(
                                "market data idle timeout"
                            ) from exc

                        if self.handle_message(raw_message):
                            received_valid_data = True
                            data_deadline = (
                                time.monotonic() + NO_DATA_TIMEOUT_SECONDS
                            )
            except asyncio.CancelledError:
                raise
            except StaleMarketData as exc:
                self.detector.clear_price_history()
                logger.warning(
                    "No current Binance market data for more than %ss (%s); "
                    "discarding the price window and reconnecting",
                    NO_DATA_TIMEOUT_SECONDS,
                    exc,
                )
            except Exception as exc:
                self.detector.clear_price_history()
                self._log_connection_error(exc)

            if stop_event.is_set():
                break
            if (
                received_valid_data
                and connected_at is not None
                and time.monotonic() - connected_at >= STABLE_CONNECTION_SECONDS
            ):
                failures = 0
            failures += 1
            delay = min(2 ** (failures - 1), MAX_RECONNECT_DELAY_SECONDS)
            logger.info("Reconnecting to Binance in %ss", delay)
            await self._wait_or_stop(stop_event, delay)

    def handle_message(self, raw_message: str | bytes) -> bool:
        trade = self._parse_trade(raw_message)
        if trade is None:
            return False
        symbol, price, event_time = trade

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
            logger.warning(
                "Price movement alert: %s direction=%s level=%s%% "
                "current=%s change=%.4f%%",
                symbol,
                alert.direction,
                f"{alert.alert_level_percent:g}",
                price,
                alert.change_percent,
            )
            self.dispatcher.enqueue(alert.as_payload())
        return True

    def _parse_trade(self, raw_message: str | bytes) -> tuple[str, float, int] | None:
        try:
            message: Any = json.loads(raw_message)
            if not isinstance(message, dict):
                raise TypeError("message is not an object")
            data = message.get("data", message)
            if not isinstance(data, dict):
                raise TypeError("data is not an object")
            if data.get("e") != "aggTrade":
                return None
            symbol = str(data["s"]).upper()
            price = float(data["p"])
            event_time = int(data["E"])
            if symbol not in self._symbol_set:
                return None
            if not math.isfinite(price) or price <= 0:
                raise ValueError("price is not finite and positive")
            return symbol, price, event_time
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            logger.warning("Ignoring malformed Binance message: %s", exc)
            return None

    @staticmethod
    async def _wait_or_stop(stop_event: asyncio.Event, delay: int) -> None:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass

    @staticmethod
    def _log_connection_error(exc: Exception) -> None:
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", None)
        if status in {403, 451}:
            logger.error(
                "Binance WebSocket rejected the connection with HTTP %s. "
                "This commonly indicates a deployment-region restriction. "
                "No alert will be generated from old data; choose a Railway "
                "region where Binance Futures is accessible.",
                status,
            )
            return
        logger.error(
            "Binance WebSocket disconnected or failed (%s: %s). Price history "
            "was cleared, so no alert can be generated from stale data.",
            type(exc).__name__,
            exc,
        )
