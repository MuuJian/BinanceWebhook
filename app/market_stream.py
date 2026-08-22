"""Resilient Binance USD-M Futures aggTrade receiver."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
from typing import Any

from websockets.asyncio.client import connect

from app.price_window import PriceWindowStore

logger = logging.getLogger(__name__)
NO_DATA_TIMEOUT_SECONDS = 10
STATUS_LOG_INTERVAL_SECONDS = 30
MAX_EVENT_AGE_MS = 10_000
MAX_RECONNECT_DELAY_SECONDS = 30
STABLE_CONNECTION_SECONDS = 60
_RECONNECT_EXPONENT_CAP = MAX_RECONNECT_DELAY_SECONDS.bit_length()


def _reconnect_delay(failures: int) -> int:
    """Return bounded exponential backoff without growing huge integers."""

    exponent = min(failures, _RECONNECT_EXPONENT_CAP)
    return min(2**exponent, MAX_RECONNECT_DELAY_SECONDS)


class StaleMarketData(ConnectionError):
    """Raised when the connection cannot provide current data for every symbol."""


class BinanceAggTradeReceiver:
    def __init__(
        self,
        *,
        websocket_url: str,
        websocket_proxy: str | None,
        symbols: tuple[str, ...],
        store: PriceWindowStore,
    ) -> None:
        self.websocket_url = websocket_url
        self.websocket_proxy = websocket_proxy
        self.symbols = symbols
        self._symbol_set = frozenset(symbols)
        self.store = store

    async def run(self, stop_event: asyncio.Event) -> None:
        failures = 0
        while not stop_event.is_set():
            connected_at: float | None = None
            try:
                proxy_mode = "configured" if self.websocket_proxy else "direct"
                region = os.getenv("RAILWAY_REGION", "local-or-unknown")
                logger.info(
                    "Connecting to Binance USD-M Futures aggTrade: "
                    "region=%s connection=%s symbols=%s",
                    region,
                    proxy_mode,
                    ",".join(self.symbols),
                )
                # proxy=None is intentional: environment proxy variables must not
                # silently alter Binance connectivity.
                async with connect(
                    self.websocket_url,
                    proxy=self.websocket_proxy,
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
                    first_seen: set[str] = set()
                    last_status_at = connected_at
                    logger.info("Binance Futures WebSocket connected")

                    while not stop_event.is_set():
                        try:
                            raw_message = await asyncio.wait_for(
                                websocket.recv(), timeout=1
                            )
                        except asyncio.TimeoutError:
                            raw_message = None

                        if raw_message is not None:
                            parsed = self._parse_message(raw_message)
                            if parsed is not None:
                                symbol, price, event_time_ms = parsed
                                now_ms = int(time.time() * 1000)
                                event_age_ms = now_ms - event_time_ms
                                if abs(event_age_ms) > MAX_EVENT_AGE_MS:
                                    raise StaleMarketData(
                                        f"{symbol} event clock age={event_age_ms}ms"
                                    )
                                if self.store.update(symbol, price, event_time_ms):
                                    last_valid_at[symbol] = time.monotonic()
                                    if symbol not in first_seen:
                                        first_seen.add(symbol)
                                        logger.info(
                                            "First aggTrade received: %s "
                                            "price=%g event_age=%dms",
                                            symbol,
                                            price,
                                            event_age_ms,
                                        )

                        now = time.monotonic()
                        silent = [
                            symbol
                            for symbol, last_seen in last_valid_at.items()
                            if now - last_seen > NO_DATA_TIMEOUT_SECONDS
                        ]
                        if silent:
                            raise StaleMarketData(
                                "no current data for " + ",".join(silent)
                            )
                        if now - last_status_at >= STATUS_LOG_INTERVAL_SECONDS:
                            self._log_status(connected_at, last_valid_at)
                            last_status_at = now
            except asyncio.CancelledError:
                raise
            except StaleMarketData as exc:
                logger.warning(
                    "Binance market data stale (%s); windows cleared before reconnect",
                    exc,
                )
            except Exception as exc:
                self._log_connection_error(exc)
            finally:
                self.store.clear_all()

            if stop_event.is_set():
                break
            if (
                connected_at is not None
                and time.monotonic() - connected_at >= STABLE_CONNECTION_SECONDS
            ):
                failures = 0
            delay = _reconnect_delay(failures)
            failures = min(failures + 1, _RECONNECT_EXPONENT_CAP)
            logger.warning("Reconnecting to Binance in %ss", delay)
            await self._wait_or_stop(stop_event, delay)

    def _parse_message(
        self, raw_message: str | bytes
    ) -> tuple[str, float, int] | None:
        try:
            message: Any = json.loads(raw_message)
            if not isinstance(message, dict):
                return None
            data = message.get("data", message)
            if not isinstance(data, dict) or data.get("e") != "aggTrade":
                return None
            raw_symbol = data["s"]
            raw_price = data["p"]
            raw_event_time = data["E"]
            if not isinstance(raw_symbol, str):
                raise ValueError("invalid symbol")
            if isinstance(raw_price, bool):
                raise ValueError("invalid price")
            if not isinstance(raw_event_time, int) or isinstance(raw_event_time, bool):
                raise ValueError("invalid event time")

            symbol = raw_symbol.upper()
            if symbol not in self._symbol_set:
                return None
            price = float(raw_price)
            event_time_ms = raw_event_time
            if not math.isfinite(price) or price <= 0 or event_time_ms <= 0:
                raise ValueError("invalid price or event time")
            return symbol, price, event_time_ms
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            logger.info("Ignoring malformed Binance message: %s", exc)
            return None

    def _log_status(
        self, connected_at: float, last_valid_at: dict[str, float]
    ) -> None:
        now = time.monotonic()
        details: list[str] = []
        for symbol in self.symbols:
            snapshot = self.store.snapshot(symbol)
            if snapshot is None:
                details.append(f"{symbol}=warming-up")
                continue
            details.append(
                f"{symbol}={snapshot.current_price:g}"
                f"(trades={snapshot.trade_count},buckets={snapshot.bucket_count},"
                f"age={now - last_valid_at[symbol]:.1f}s)"
            )
        logger.info(
            "Binance aggTrade healthy (connected=%ds): %s",
            int(now - connected_at),
            " ".join(details),
        )

    @staticmethod
    async def _wait_or_stop(stop_event: asyncio.Event, delay: float) -> None:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass

    def _log_connection_error(self, exc: Exception) -> None:
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", None)
        if status in {403, 451}:
            logger.error(
                "Binance rejected WebSocket with HTTP %s; this deployment region "
                "may not permit Futures access. No stale-price alert was sent.",
                status,
            )
            return
        if self.websocket_proxy:
            logger.error(
                "Binance WebSocket proxy connection failed (%s); windows cleared",
                type(exc).__name__,
            )
            return
        logger.error(
            "Binance WebSocket disconnected or failed (%s: %s); windows cleared",
            type(exc).__name__,
            exc,
        )
