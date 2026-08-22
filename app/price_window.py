"""Memory-bounded rolling market activity, compacted per second."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass(slots=True)
class _Bucket:
    second: int
    close: float
    last_time_ms: int
    trades: int = 1


@dataclass(frozen=True, slots=True)
class WindowSnapshot:
    generation: int
    current_price: float
    latest_event_time_ms: int
    trade_count: int
    bucket_count: int


@dataclass(frozen=True, slots=True)
class LatestPriceSnapshot:
    generation: int
    current_price: float
    latest_event_time_ms: int


class PriceWindow:
    """Retain recent activity without storing every individual trade."""

    def __init__(self, window_seconds: int) -> None:
        self.window_seconds = window_seconds
        self._buckets: deque[_Bucket] = deque()
        self._trade_count = 0
        self.generation = 0

    def clear(self) -> None:
        self._buckets.clear()
        self._trade_count = 0
        self.generation += 1

    def update(self, price: float, event_time_ms: int) -> bool:
        if self._buckets and event_time_ms < self._buckets[-1].last_time_ms:
            return False

        self._trade_count += 1
        second = event_time_ms // 1000
        if self._buckets and self._buckets[-1].second == second:
            bucket = self._buckets[-1]
            bucket.close = price
            bucket.last_time_ms = event_time_ms
            bucket.trades += 1
        else:
            self._buckets.append(
                _Bucket(
                    second=second,
                    close=price,
                    last_time_ms=event_time_ms,
                )
            )

        cutoff_second = second - self.window_seconds
        while self._buckets and self._buckets[0].second <= cutoff_second:
            self._trade_count -= self._buckets.popleft().trades
        return True

    def snapshot(self) -> WindowSnapshot | None:
        if not self._buckets:
            return None
        latest = self._buckets[-1]
        return WindowSnapshot(
            generation=self.generation,
            current_price=latest.close,
            latest_event_time_ms=latest.last_time_ms,
            trade_count=self._trade_count,
            bucket_count=len(self._buckets),
        )

    def latest(self) -> LatestPriceSnapshot | None:
        """Return the latest price in O(1) without scanning the whole window."""

        if not self._buckets:
            return None
        latest = self._buckets[-1]
        return LatestPriceSnapshot(
            generation=self.generation,
            current_price=latest.close,
            latest_event_time_ms=latest.last_time_ms,
        )


class PriceWindowStore:
    def __init__(self, symbols: tuple[str, ...], window_seconds: int) -> None:
        self._windows = {
            symbol: PriceWindow(window_seconds) for symbol in symbols
        }

    def update(self, symbol: str, price: float, event_time_ms: int) -> bool:
        return self._windows[symbol].update(price, event_time_ms)

    def snapshot(self, symbol: str) -> WindowSnapshot | None:
        return self._windows[symbol].snapshot()

    def latest(self, symbol: str) -> LatestPriceSnapshot | None:
        return self._windows[symbol].latest()

    def clear(self, symbol: str) -> None:
        self._windows[symbol].clear()

    def clear_all(self) -> None:
        for window in self._windows.values():
            window.clear()
