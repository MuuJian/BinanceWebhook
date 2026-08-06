"""In-memory event-time price windows for the fixed Binance symbols."""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PricePoint:
    event_time_ms: int
    price: float


@dataclass(frozen=True, slots=True)
class PriceSnapshot:
    points: tuple[PricePoint, ...]

    @property
    def current_price(self) -> float:
        return self.points[-1].price

    @property
    def lowest_price(self) -> float:
        return min(point.price for point in self.points)

    @property
    def highest_price(self) -> float:
        return max(point.price for point in self.points)

    @property
    def span_seconds(self) -> float:
        return (self.points[-1].event_time_ms - self.points[0].event_time_ms) / 1000


class PriceWindowStore:
    """Maintain one ordered 120-second-style deque per symbol."""

    def __init__(self, symbols: tuple[str, ...], window_seconds: int) -> None:
        if not symbols:
            raise ValueError("symbols must not be empty")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be greater than zero")
        self.symbols = symbols
        self.window_seconds = window_seconds
        self._windows = {symbol: deque[PricePoint]() for symbol in symbols}

    def update(
        self,
        symbol: str,
        price: float,
        event_time_ms: int,
    ) -> bool:
        """Add a current point, returning False for unsupported or old data."""

        if symbol not in self._windows:
            return False
        if not math.isfinite(price) or price <= 0:
            raise ValueError("price must be a finite number greater than zero")
        if event_time_ms <= 0:
            raise ValueError("event_time_ms must be greater than zero")

        window = self._windows[symbol]
        if window and event_time_ms < window[-1].event_time_ms:
            return False

        point = PricePoint(event_time_ms=event_time_ms, price=price)
        if window and event_time_ms == window[-1].event_time_ms:
            window[-1] = point
        else:
            window.append(point)

        cutoff_ms = event_time_ms - self.window_seconds * 1000
        while window and window[0].event_time_ms < cutoff_ms:
            window.popleft()
        return True

    def snapshot(self, symbol: str) -> PriceSnapshot | None:
        window = self._windows.get(symbol)
        if not window:
            return None
        return PriceSnapshot(tuple(window))

    def clear(self, symbol: str) -> None:
        if symbol in self._windows:
            self._windows[symbol].clear()

    def clear_all(self) -> None:
        for symbol in self.symbols:
            self.clear(symbol)
