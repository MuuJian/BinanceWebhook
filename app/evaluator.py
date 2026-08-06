"""Once-per-second volatility evaluation and direction-specific cooldowns."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Literal

from app.price_window import PriceSnapshot, PriceWindowStore

logger = logging.getLogger(__name__)
ALERT_LOG_LEVEL = 35
Direction = Literal["up", "down"]


@dataclass(frozen=True, slots=True)
class Alert:
    symbol: str
    direction: Direction
    change_pct: float
    current_price: float
    reference_price: float
    window_seconds: int

    @property
    def message(self) -> str:
        direction_text = "上涨" if self.direction == "up" else "下跌"
        if self.window_seconds % 60 == 0:
            window_text = f"{self.window_seconds // 60}分钟内"
        else:
            window_text = f"{self.window_seconds}秒内"
        return (
            f"{self.symbol} 合约{window_text}{direction_text}"
            f"{abs(self.change_pct):.2f}%，当前价格{self.current_price:.2f}，"
            f"参考价格{self.reference_price:.2f}"
        )


class PriceEvaluator:
    def __init__(
        self,
        *,
        store: PriceWindowStore,
        alert_queue: asyncio.Queue[Alert],
        symbols: tuple[str, ...],
        window_seconds: int,
        threshold_pct: float,
        cooldown_seconds: float,
        evaluation_interval_seconds: float,
        min_points: int,
        warmup_seconds: float,
    ) -> None:
        self.store = store
        self.alert_queue = alert_queue
        self.symbols = symbols
        self.window_seconds = window_seconds
        self.threshold_pct = threshold_pct
        self.cooldown_seconds = cooldown_seconds
        self.evaluation_interval_seconds = evaluation_interval_seconds
        self.min_points = min_points
        self.warmup_seconds = warmup_seconds
        self._last_alert_at: dict[tuple[str, Direction], float] = {}

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            evaluation_started = time.monotonic()
            for symbol in self.symbols:
                self.evaluate_symbol(symbol, now=evaluation_started)
            elapsed = time.monotonic() - evaluation_started
            await self._wait_or_stop(
                stop_event,
                max(0.0, self.evaluation_interval_seconds - elapsed),
            )

    def evaluate_symbol(self, symbol: str, *, now: float | None = None) -> None:
        snapshot = self.store.snapshot(symbol)
        if not self._is_ready(snapshot):
            return
        assert snapshot is not None

        current_price = snapshot.current_price
        lowest_price = snapshot.lowest_price
        highest_price = snapshot.highest_price
        up_pct = (current_price - lowest_price) / lowest_price * 100
        down_pct = (current_price - highest_price) / highest_price * 100
        evaluated_at = time.monotonic() if now is None else now

        if up_pct >= self.threshold_pct:
            self._trigger_or_skip(
                Alert(
                    symbol=symbol,
                    direction="up",
                    change_pct=up_pct,
                    current_price=current_price,
                    reference_price=lowest_price,
                    window_seconds=self.window_seconds,
                ),
                evaluated_at,
            )
        if down_pct <= -self.threshold_pct:
            self._trigger_or_skip(
                Alert(
                    symbol=symbol,
                    direction="down",
                    change_pct=down_pct,
                    current_price=current_price,
                    reference_price=highest_price,
                    window_seconds=self.window_seconds,
                ),
                evaluated_at,
            )

    def _is_ready(self, snapshot: PriceSnapshot | None) -> bool:
        return (
            snapshot is not None
            and len(snapshot.points) >= self.min_points
            and snapshot.span_seconds >= self.warmup_seconds
        )

    def _trigger_or_skip(self, alert: Alert, now: float) -> None:
        key = (alert.symbol, alert.direction)
        last_alert_at = self._last_alert_at.get(key)
        if (
            last_alert_at is not None
            and now - last_alert_at < self.cooldown_seconds
        ):
            remaining = self.cooldown_seconds - (now - last_alert_at)
            logger.warning(
                "%s %s alert blocked by cooldown (remaining=%.1fs)",
                alert.symbol,
                alert.direction,
                remaining,
            )
            return

        try:
            self.alert_queue.put_nowait(alert)
        except asyncio.QueueFull:
            logger.error(
                "Alert queue full; dropping %s %s alert",
                alert.symbol,
                alert.direction,
            )
            return

        self._last_alert_at[key] = now
        logger.log(ALERT_LOG_LEVEL, "%s", alert.message)

    @staticmethod
    async def _wait_or_stop(stop_event: asyncio.Event, delay: float) -> None:
        if delay <= 0:
            await asyncio.sleep(0)
            return
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass
