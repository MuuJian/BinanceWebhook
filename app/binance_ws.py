"""Resilient Binance USD-M Futures miniTicker receiver."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from typing import Any

from websockets.asyncio.client import connect

from app.price_window import PriceWindowStore

logger = logging.getLogger(__name__)
NO_DATA_TIMEOUT_SECONDS = 10
MAX_EVENT_AGE_MS = 10_000
MAX_RECONNECT_DELAY_SECONDS = 30
STABLE_CONNECTION_SECONDS = 60


class StaleMarketData(ConnectionError):
    """Raised when the stream is connected but cannot provide current prices."""


class BinanceMiniTickerReceiver:
    def __init__(
        self,
        *,
        websocket_url: str,
        symbols: tuple[str, ...],
        store: PriceWindowStore,
    ) -> None:
        self.websocket_url = websocket_url
        self.symbols = symbols
        self._symbol_set = frozenset(symbols)
        self.store = store

    async def run(self, stop_event: asyncio.Event) -> None:
        failures = 0
        while not stop_event.is_set():
            self.store.clear_all()
            connected_at: float | None = None
            last_valid_at: dict[str, float] = {}

            try:
                logger.info("Connecting to Binance USD-M Futures miniTicker stream")
                async with connect(
                    self.websocket_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=10,
                    open_timeout=15,
                    max_queue=1024,
                ) as websocket:
                    connected_at = time.monotonic()
                    last_valid_at = {
                        symbol: connected_at for symbol in self.symbols
                    }
                    logger.info("Binance Futures WebSocket connected")

                    while not stop_event.is_set():
                        try:
                            raw_message = await asyncio.wait_for(
                                websocket.recv(), timeout=1
                            )
                        except asyncio.TimeoutError:
                            raw_message = None

                        if raw_message is not None:
                            symbol = self.handle_message(raw_message)
                            if symbol is not None:
                                last_valid_at[symbol] = time.monotonic()

                        now = time.monotonic()
                        silent_symbols = tuple(
                            symbol
                            for symbol, last_seen in last_valid_at.items()
                            if now - last_seen > NO_DATA_TIMEOUT_SECONDS
                        )
                        if silent_symbols:
                            for symbol in silent_symbols:
                                self.store.clear(symbol)
                            raise StaleMarketData(
                                "no current data for " + ",".join(silent_symbols)
                            )
            except asyncio.CancelledError:
                raise
            except StaleMarketData as exc:
                self.store.clear_all()
                logger.warning(
                    "No current Binance data for more than %ss (%s); "
                    "price windows cleared before reconnect",
                    NO_DATA_TIMEOUT_SECONDS,
                    exc,
                )
            except Exception as exc:
                self.store.clear_all()
                self._log_connection_error(exc)

            if stop_event.is_set():
                break
            if (
                connected_at is not None
                and time.monotonic() - connected_at >= STABLE_CONNECTION_SECONDS
            ):
                failures = 0
            delay = min(2**failures, MAX_RECONNECT_DELAY_SECONDS)
            failures += 1
            logger.info("Reconnecting to Binance in %ss", delay)
            await self._wait_or_stop(stop_event, delay)

    def handle_message(self, raw_message: str | bytes) -> str | None:
        ticker = self._parse_message(raw_message)
        if ticker is None:
            return None
        symbol, price, event_time_ms = ticker

        event_age_ms = int(time.time() * 1000) - event_time_ms
        if event_age_ms > MAX_EVENT_AGE_MS:
            raise StaleMarketData(
                f"received {symbol} event that is {event_age_ms}ms old"
            )

        if not self.store.update(symbol, price, event_time_ms):
            logger.warning("Ignoring out-of-order Binance event for %s", symbol)
            return None
        logger.debug("%s latest price %.8f", symbol, price)
        return symbol

    def _parse_message(
        self, raw_message: str | bytes
    ) -> tuple[str, float, int] | None:
        try:
            message: Any = json.loads(raw_message)
            if not isinstance(message, dict):
                raise TypeError("message is not an object")
            data = message.get("data", message)
            if not isinstance(data, dict):
                raise TypeError("data is not an object")
            if data.get("e") != "24hrMiniTicker":
                return None
            symbol = str(data["s"]).upper()
            price = float(data["c"])
            event_time_ms = int(data["E"])
            if symbol not in self._symbol_set:
                return None
            if not math.isfinite(price) or price <= 0:
                raise ValueError("price is not finite and positive")
            if event_time_ms <= 0:
                raise ValueError("event time is not positive")
            return symbol, price, event_time_ms
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            logger.warning("Ignoring malformed Binance message: %s", exc)
            return None

    @staticmethod
    async def _wait_or_stop(stop_event: asyncio.Event, delay: float) -> None:
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
                "Binance WebSocket rejected the connection with HTTP %s; "
                "the deployment region may not permit Binance Futures. "
                "No stale-price alert will be sent.",
                status,
            )
            return
        logger.error(
            "Binance WebSocket disconnected or failed (%s: %s); "
            "price windows were cleared",
            type(exc).__name__,
            exc,
        )
