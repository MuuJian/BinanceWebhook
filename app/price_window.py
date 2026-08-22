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
    current_price: float
    trade_count: int
    bucket_count: int


class PriceWindow:
    """Retain recent activity without storing every individual trade."""

    def __init__(self, window_seconds: int) -> None:
        self.window_seconds = window_seconds
        self._buckets: deque[_Bucket] = deque()
        self._trade_count = 0

    def clear(self) -> None:
        self._buckets.clear()
        self._trade_count = 0

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
            current_price=latest.close,
            trade_count=self._trade_count,
            bucket_count=len(self._buckets),
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

    def clear_all(self) -> None:
        for window in self._windows.values():
            window.clear()
