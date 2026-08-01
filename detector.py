"""Efficient per-symbol rolling-window price movement detection."""

from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

COOLDOWN_SECONDS = 30
_EPSILON = 1e-9

Direction = Literal["up", "down"]


@dataclass(frozen=True, slots=True)
class Alert:
    symbol: str
    direction: Direction
    alert_level_percent: float
    current_price: float
    peak_price: float
    lowest_price: float
    change_percent: float
    drop_percent: float
    rise_percent: float
    window_seconds: int
    event_time: int
    triggered_at: str

    def as_payload(self) -> dict[str, object]:
        direction_text = "上涨" if self.direction == "up" else "下跌"
        window_text = (
            f"{self.window_seconds // 60}分钟内"
            if self.window_seconds % 60 == 0
            else f"{self.window_seconds}秒内"
        )
        message = (
            f"{self.symbol} {window_text}{direction_text}"
            f"{self.alert_level_percent:g}%"
        )
        reference_price = (
            self.lowest_price if self.direction == "up" else self.peak_price
        )
        return {
            "event": "price_change_alert",
            "market": f"BINANCE:{self.symbol}",
            "alert name": message,
            "message": message,
            "direction": self.direction,
            "alertLevelPercent": self.alert_level_percent,
            "symbol": self.symbol,
            "currentPrice": self.current_price,
            "referencePrice": reference_price,
            "peakPrice": self.peak_price,
            "lowestPrice": self.lowest_price,
            "changePercent": round(self.change_percent, 6),
            "dropPercent": round(self.drop_percent, 6),
            "risePercent": round(self.rise_percent, 6),
            "windowSeconds": self.window_seconds,
            "eventTime": self.event_time,
            "triggeredAt": self.triggered_at,
        }


@dataclass(frozen=True, slots=True)
class _PriceSample:
    sequence: int
    observed_at: float
    price: float


class _RollingPriceWindow:
    """Keep all recent prices while maintaining O(1) rolling extrema."""

    def __init__(self, window_seconds: int) -> None:
        self.window_seconds = window_seconds
        self._sequence = 0
        self._points: deque[_PriceSample] = deque()
        self._highs: deque[_PriceSample] = deque()
        self._lows: deque[_PriceSample] = deque()

    def append(self, observed_at: float, price: float) -> float | None:
        self._expire(observed_at - self.window_seconds)
        previous_price = self._points[-1].price if self._points else None

        self._sequence += 1
        sample = _PriceSample(self._sequence, observed_at, price)
        self._points.append(sample)

        while self._highs and self._highs[-1].price <= price:
            self._highs.pop()
        self._highs.append(sample)

        while self._lows and self._lows[-1].price >= price:
            self._lows.pop()
        self._lows.append(sample)
        return previous_price

    @property
    def highest(self) -> float:
        return self._highs[0].price

    @property
    def lowest(self) -> float:
        return self._lows[0].price

    def clear(self) -> None:
        self._points.clear()
        self._highs.clear()
        self._lows.clear()

    def _expire(self, cutoff: float) -> None:
        while self._points and self._points[0].observed_at < cutoff:
            expired = self._points.popleft()
            if self._highs and self._highs[0].sequence == expired.sequence:
                self._highs.popleft()
            if self._lows and self._lows[0].sequence == expired.sequence:
                self._lows.popleft()


@dataclass(slots=True)
class _AlertState:
    alerted_up_index: int = -1
    alerted_down_index: int = -1
    last_alert_at: float | None = None
    pending_direction: Direction | None = None
    pending_index: int = -1


class PriceMovementDetector:
    """Detect configured upward and downward movement levels."""

    def __init__(
        self,
        *,
        symbols: tuple[str, ...],
        window_seconds: int,
        change_levels: tuple[float, ...],
    ) -> None:
        if window_seconds <= 0:
            raise ValueError("window_seconds must be greater than zero")
        if not change_levels:
            raise ValueError("change_levels must not be empty")
        if any(not math.isfinite(level) or level <= 0 for level in change_levels):
            raise ValueError("change_levels must contain finite positive numbers")
        if any(
            current <= previous
            for previous, current in zip(change_levels, change_levels[1:])
        ):
            raise ValueError("change_levels must be strictly increasing")

        self.symbols = frozenset(symbols)
        self.window_seconds = window_seconds
        self.change_levels = change_levels
        self._windows = {
            symbol: _RollingPriceWindow(window_seconds) for symbol in symbols
        }
        self._states = {symbol: _AlertState() for symbol in symbols}

    def clear_price_history(self) -> None:
        """Drop feed-derived state without bypassing the 30-second rest."""

        for window in self._windows.values():
            window.clear()
        for state in self._states.values():
            state.alerted_up_index = -1
            state.alerted_down_index = -1
            state.pending_direction = None
            state.pending_index = -1

    def process(
        self,
        *,
        symbol: str,
        price: float,
        event_time: int,
        observed_at: float | None = None,
    ) -> Alert | None:
        if symbol not in self.symbols:
            return None
        if not math.isfinite(price) or price <= 0:
            raise ValueError("price must be a finite number greater than zero")

        now = time.monotonic() if observed_at is None else observed_at
        window = self._windows[symbol]
        previous_price = window.append(now, price)
        peak = window.highest
        lowest = window.lowest
        drop = (price / peak - 1) * 100
        rise = (price / lowest - 1) * 100
        state = self._states[symbol]

        self._reset_recovered_directions(state, rise=rise, drop=drop)
        cooldown_finished = (
            state.last_alert_at is None
            or now - state.last_alert_at >= COOLDOWN_SECONDS
        )

        pending = self._validated_pending(state, rise=rise, drop=drop)
        if pending is not None and cooldown_finished:
            direction, crossed_index = pending
            return self._create_alert(
                state=state,
                symbol=symbol,
                direction=direction,
                crossed_index=crossed_index,
                price=price,
                peak=peak,
                lowest=lowest,
                rise=rise,
                drop=drop,
                event_time=event_time,
                now=now,
            )

        if previous_price is None or price == previous_price:
            return None

        direction: Direction = "up" if price > previous_price else "down"
        change = rise if direction == "up" else drop
        crossed_index = self._crossed_index(abs(change))
        if crossed_index is None or crossed_index <= self._alerted_index(
            state, direction
        ):
            return None

        if not cooldown_finished:
            self._store_pending(state, direction, crossed_index)
            return None

        return self._create_alert(
            state=state,
            symbol=symbol,
            direction=direction,
            crossed_index=crossed_index,
            price=price,
            peak=peak,
            lowest=lowest,
            rise=rise,
            drop=drop,
            event_time=event_time,
            now=now,
        )

    def _reset_recovered_directions(
        self, state: _AlertState, *, rise: float, drop: float
    ) -> None:
        first_level = self.change_levels[0]
        if rise + _EPSILON < first_level:
            state.alerted_up_index = -1
            if state.pending_direction == "up":
                self._clear_pending(state)
        if abs(drop) + _EPSILON < first_level:
            state.alerted_down_index = -1
            if state.pending_direction == "down":
                self._clear_pending(state)

    def _validated_pending(
        self, state: _AlertState, *, rise: float, drop: float
    ) -> tuple[Direction, int] | None:
        direction = state.pending_direction
        if direction is None:
            return None
        change = rise if direction == "up" else drop
        crossed_index = self._crossed_index(abs(change))
        if crossed_index is None or crossed_index <= self._alerted_index(
            state, direction
        ):
            self._clear_pending(state)
            return None
        state.pending_index = crossed_index
        return direction, crossed_index

    def _store_pending(
        self, state: _AlertState, direction: Direction, crossed_index: int
    ) -> None:
        current_level = (
            self.change_levels[state.pending_index]
            if state.pending_direction is not None and state.pending_index >= 0
            else -1
        )
        if self.change_levels[crossed_index] >= current_level:
            state.pending_direction = direction
            state.pending_index = crossed_index

    def _crossed_index(self, magnitude: float) -> int | None:
        for index in range(len(self.change_levels) - 1, -1, -1):
            if magnitude + _EPSILON >= self.change_levels[index]:
                return index
        return None

    @staticmethod
    def _alerted_index(state: _AlertState, direction: Direction) -> int:
        return (
            state.alerted_up_index
            if direction == "up"
            else state.alerted_down_index
        )

    @staticmethod
    def _clear_pending(state: _AlertState) -> None:
        state.pending_direction = None
        state.pending_index = -1

    def _create_alert(
        self,
        *,
        state: _AlertState,
        symbol: str,
        direction: Direction,
        crossed_index: int,
        price: float,
        peak: float,
        lowest: float,
        rise: float,
        drop: float,
        event_time: int,
        now: float,
    ) -> Alert:
        if direction == "up":
            state.alerted_up_index = crossed_index
            change = rise
        else:
            state.alerted_down_index = crossed_index
            change = drop
        state.last_alert_at = now
        self._clear_pending(state)

        return Alert(
            symbol=symbol,
            direction=direction,
            alert_level_percent=self.change_levels[crossed_index],
            current_price=price,
            peak_price=peak,
            lowest_price=lowest,
            change_percent=change,
            drop_percent=drop,
            rise_percent=rise,
            window_seconds=self.window_seconds,
            event_time=event_time,
            triggered_at=datetime.now(timezone.utc).isoformat(),
        )
