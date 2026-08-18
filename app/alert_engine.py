"""Evaluate rolling price movement and create progressive text alerts."""

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
    tier: int
    movement_pct: float
    message: str


@dataclass(slots=True)
class _DirectionState:
    generation: int = -1
    highest_sent_tier: int = 0
    reset_below_since: float | None = None
    last_sent_monotonic: float = -math.inf


class AlertEngine:
    def __init__(
        self,
        *,
        symbols: tuple[str, ...],
        store: PriceWindowStore,
        queue: asyncio.Queue[Alert],
        threshold_pct: float,
        reset_pct: float,
        reset_confirm_seconds: float,
        cooldown_seconds: float,
        evaluation_interval_seconds: float,
        min_points: int,
        warmup_seconds: float,
        window_seconds: int,
    ) -> None:
        self.symbols = symbols
        self.store = store
        self.queue = queue
        self.threshold_pct = threshold_pct
        self.reset_pct = reset_pct
        self.reset_confirm_seconds = reset_confirm_seconds
        self.cooldown_seconds = cooldown_seconds
        self.evaluation_interval_seconds = evaluation_interval_seconds
        self.min_points = min_points
        self.warmup_seconds = warmup_seconds
        self.window_seconds = window_seconds
        self._states = {
            (symbol, direction): _DirectionState()
            for symbol in symbols
            for direction in ("up", "down")
        }

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
        for symbol in self.symbols:
            snapshot = self.store.snapshot(symbol)
            if snapshot is None:
                continue
            if (
                snapshot.trade_count < self.min_points
                or snapshot.span_seconds < self.warmup_seconds
            ):
                continue

            up_pct = (
                snapshot.current_price / snapshot.lowest_price - 1
            ) * 100
            down_pct = (
                1 - snapshot.current_price / snapshot.highest_price
            ) * 100
            movements = {"up": up_pct, "down": down_pct}
            for direction, movement in movements.items():
                self._update_reset_state(
                    symbol,
                    direction,
                    movement,
                    snapshot.generation,
                    now,
                )

            candidates: list[tuple[str, float, float, int]] = []
            if up_pct >= self.threshold_pct:
                candidates.append(
                    ("up", up_pct, snapshot.lowest_price, snapshot.low_event_time_ms)
                )
            if down_pct >= self.threshold_pct:
                candidates.append(
                    (
                        "down",
                        down_pct,
                        snapshot.highest_price,
                        snapshot.high_event_time_ms,
                    )
                )

            if len(candidates) == 2:
                candidates.sort(key=lambda item: (item[3], item[1]), reverse=True)
                candidates = candidates[:1]

            for direction, movement, reference, _ in candidates:
                self._consider(
                    symbol, direction, movement, reference, snapshot, now
                )

    def _state(
        self, symbol: str, direction: str, generation: int
    ) -> _DirectionState:
        state = self._states[(symbol, direction)]
        if state.generation != generation:
            state.generation = generation
            state.highest_sent_tier = 0
            state.reset_below_since = None
        return state

    def _update_reset_state(
        self,
        symbol: str,
        direction: str,
        movement: float,
        generation: int,
        now: float,
    ) -> None:
        state = self._state(symbol, direction, generation)
        if state.highest_sent_tier == 0:
            state.reset_below_since = None
            return
        if movement > self.reset_pct:
            state.reset_below_since = None
            return
        if state.reset_below_since is None:
            state.reset_below_since = now
            return
        if now - state.reset_below_since < self.reset_confirm_seconds:
            return

        previous_tier = state.highest_sent_tier
        state.highest_sent_tier = 0
        state.reset_below_since = None
        # Preserve the send timestamp so a new round cannot bypass the hard
        # cooldown and place two calls less than COOLDOWN_SECONDS apart.
        logger.info(
            "Alert state reset: symbol=%s direction=%s previous_level=%g%%",
            symbol,
            direction,
            previous_tier * self.threshold_pct,
        )

    def _consider(
        self,
        symbol: str,
        direction: str,
        movement: float,
        reference: float,
        snapshot: WindowSnapshot,
        now: float,
    ) -> None:
        state = self._state(symbol, direction, snapshot.generation)
        next_tier = state.highest_sent_tier + 1
        next_threshold = next_tier * self.threshold_pct
        if movement + 1e-12 < next_threshold:
            return
        if now - state.last_sent_monotonic < self.cooldown_seconds:
            return

        send_tier = next_tier
        action = "上涨" if direction == "up" else "下跌"
        minutes = self.window_seconds / 60
        window_label = f"{minutes:g}分钟"
        message = (
            f"{symbol} 合约{window_label}内{action}{movement:.2f}%，"
            f"触发{next_threshold:g}%档提醒，当前价格{snapshot.current_price:g}，"
            f"参考价格{reference:g}"
        )
        alert = Alert(
            symbol=symbol,
            price=snapshot.current_price,
            direction=direction,
            tier=send_tier,
            movement_pct=movement,
            message=message,
        )
        try:
            self.queue.put_nowait(alert)
        except asyncio.QueueFull:
            logger.error("Webhook queue is full; alert dropped for %s", symbol)
            return
        state.highest_sent_tier = send_tier
        state.last_sent_monotonic = now
        logger.info(
            "Price alert queued: symbol=%s direction=%s level=%g%% movement=%.2f%%",
            symbol,
            direction,
            send_tier * self.threshold_pct,
            movement,
        )
