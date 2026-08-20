"""Evaluate rolling price movement with a global alert cooldown."""

from __future__ import annotations

import asyncio
import logging
import math
import time
from dataclasses import dataclass

from app.price_window import PriceWindowStore, WindowSnapshot

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Alert:
    symbol: str
    price: float
    direction: str
    movement_pct: float


class AlertEngine:
    def __init__(
        self,
        *,
        symbols: tuple[str, ...],
        store: PriceWindowStore,
        queue: asyncio.Queue[Alert],
        threshold_pct: float,
        cooldown_seconds: float,
        evaluation_interval_seconds: float,
        min_points: int,
        warmup_seconds: float,
    ) -> None:
        self.symbols = symbols
        self.store = store
        self.queue = queue
        self.threshold_pct = threshold_pct
        self.cooldown_seconds = cooldown_seconds
        self.evaluation_interval_seconds = evaluation_interval_seconds
        self.min_points = min_points
        self.warmup_seconds = warmup_seconds
        self._cooldown_until = -math.inf

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            self.evaluate_once()
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=self.evaluation_interval_seconds
                )
            except asyncio.TimeoutError:
                pass

    def evaluate_once(self) -> None:
        now = time.monotonic()
        if now < self._cooldown_until:
            return

        candidates: list[tuple[int, float, str, WindowSnapshot, str]] = []
        for symbol in self.symbols:
            snapshot = self.store.snapshot(symbol)
            if not self._is_ready(snapshot):
                continue
            assert snapshot is not None

            candidate = self._movement_candidate(snapshot)
            if candidate is None:
                continue
            direction, movement, reference_time_ms = candidate
            candidates.append(
                (reference_time_ms, movement, symbol, snapshot, direction)
            )

        if not candidates:
            return

        _, movement, symbol, snapshot, direction = max(candidates)
        alert = Alert(
            symbol=symbol,
            price=snapshot.current_price,
            direction=direction,
            movement_pct=movement,
        )
        try:
            self.queue.put_nowait(alert)
        except asyncio.QueueFull:
            logger.error("Webhook queue is full; alert dropped for %s", symbol)
            return

        self._cooldown_until = now + self.cooldown_seconds
        logger.info(
            "Price alert queued: symbol=%s direction=%s movement=%.2f%%; "
            "all alerts paused for %gs",
            symbol,
            direction,
            movement,
            self.cooldown_seconds,
        )

    def _is_ready(self, snapshot: WindowSnapshot | None) -> bool:
        return bool(
            snapshot is not None
            and snapshot.trade_count >= self.min_points
            and snapshot.span_seconds >= self.warmup_seconds
        )

    def _movement_candidate(
        self, snapshot: WindowSnapshot
    ) -> tuple[str, float, int] | None:
        up_pct = (snapshot.current_price / snapshot.lowest_price - 1) * 100
        down_pct = (1 - snapshot.current_price / snapshot.highest_price) * 100
        candidates: list[tuple[str, float, int]] = []
        if up_pct >= self.threshold_pct:
            candidates.append(("up", up_pct, snapshot.low_event_time_ms))
        if down_pct >= self.threshold_pct:
            candidates.append(("down", down_pct, snapshot.high_event_time_ms))
        if not candidates:
            return None

        # If both directions exceed the threshold, the more recent extreme
        # describes the move that led to the current price.
        direction, movement, reference_time_ms = max(
            candidates, key=lambda item: (item[2], item[1])
        )
        return direction, movement, reference_time_ms
