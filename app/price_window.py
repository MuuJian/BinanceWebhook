"""Memory-bounded rolling price windows compressed into one bucket per second."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass(slots=True)
class _Bucket:
    second: int
    close: float
    high: float
    low: float
    first_time_ms: int
    high_time_ms: int
    low_time_ms: int
    last_time_ms: int
    trades: int = 1


@dataclass(frozen=True, slots=True)
class WindowSnapshot:
    generation: int
    current_price: float
    highest_price: float
    lowest_price: float
    high_event_time_ms: int
    low_event_time_ms: int
    latest_event_time_ms: int
    trade_count: int
    bucket_count: int
    span_seconds: float


@dataclass(frozen=True, slots=True)
class LatestPriceSnapshot:
    generation: int
    current_price: float
    latest_event_time_ms: int


class PriceWindow:
    """A price window that retains OHLC extremes without retaining every trade."""

    def __init__(self, window_seconds: int) -> None:
        self.window_seconds = window_seconds
        self._buckets: deque[_Bucket] = deque()
        self.generation = 0

    def clear(self) -> None:
        self._buckets.clear()
        self.generation += 1

    def update(self, price: float, event_time_ms: int) -> bool:
        if self._buckets and event_time_ms < self._buckets[-1].last_time_ms:
            return False

        second = event_time_ms // 1000
        if self._buckets and self._buckets[-1].second == second:
            bucket = self._buckets[-1]
            bucket.close = price
            bucket.last_time_ms = event_time_ms
            bucket.trades += 1
            if price > bucket.high:
                bucket.high = price
                bucket.high_time_ms = event_time_ms
            if price < bucket.low:
                bucket.low = price
                bucket.low_time_ms = event_time_ms
        else:
            self._buckets.append(
                _Bucket(
                    second=second,
                    close=price,
                    high=price,
                    low=price,
                    first_time_ms=event_time_ms,
                    high_time_ms=event_time_ms,
                    low_time_ms=event_time_ms,
                    last_time_ms=event_time_ms,
                )
            )

        cutoff_second = second - self.window_seconds
        while self._buckets and self._buckets[0].second <= cutoff_second:
            self._buckets.popleft()
        return True

    def snapshot(self) -> WindowSnapshot | None:
        if not self._buckets:
            return None
        latest = self._buckets[-1]
        high_bucket = max(self._buckets, key=lambda item: item.high)
        low_bucket = min(self._buckets, key=lambda item: item.low)
        first_event_ms = self._buckets[0].first_time_ms
        return WindowSnapshot(
            generation=self.generation,
            current_price=latest.close,
            highest_price=high_bucket.high,
            lowest_price=low_bucket.low,
            high_event_time_ms=high_bucket.high_time_ms,
            low_event_time_ms=low_bucket.low_time_ms,
            latest_event_time_ms=latest.last_time_ms,
            trade_count=sum(bucket.trades for bucket in self._buckets),
            bucket_count=len(self._buckets),
            span_seconds=max(0.0, (latest.last_time_ms - first_event_ms) / 1000),
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
