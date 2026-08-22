"""Evaluate every accepted trade against fixed per-symbol price anchors."""

from __future__ import annotations

import asyncio
import logging
import math
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Alert:
    symbol: str
    price: float
    direction: str
    movement_pct: float


@dataclass(slots=True)
class _AnchorState:
    anchor_price: float | None = None
    anchor_event_time_ms: int = 0
    current_price: float | None = None
    latest_event_time_ms: int = 0
    direction: str = "up"
    movement_pct: float = 0.0
    pending_since_ms: int | None = None


class AlertEngine:
    def __init__(
        self,
        *,
        symbols: tuple[str, ...],
        queue: asyncio.Queue[Alert],
        threshold_pct: float,
        cooldown_seconds: float,
        window_seconds: int,
    ) -> None:
        self.symbols = symbols
        self.queue = queue
        self.threshold_pct = threshold_pct
        self.cooldown_seconds = cooldown_seconds
        self.window_ms = window_seconds * 1000
        self._cooldown_until = -math.inf
        self._queue_blocked = False
        self._states = {symbol: _AnchorState() for symbol in symbols}

    def observe(self, symbol: str, price: float, event_time_ms: int) -> None:
        """Process one accepted Binance trade immediately."""

        now = time.monotonic()
        state = self._states[symbol]
        state.current_price = price
        state.latest_event_time_ms = event_time_ms

        if state.anchor_price is None:
            self._set_anchor(symbol, state, reason="initialized")
            self._dispatch_pending(now)
            return

        direction, movement = self._movement(state.anchor_price, price)
        state.direction = direction
        state.movement_pct = movement
        if movement >= self.threshold_pct:
            if state.pending_since_ms is None:
                state.pending_since_ms = event_time_ms
        else:
            state.pending_since_ms = None
            if event_time_ms - state.anchor_event_time_ms >= self.window_ms:
                self._set_anchor(symbol, state, reason="window expired")

        # Anchor maintenance never pauses. Only dispatch is gated by cooldown.
        self._dispatch_pending(now)

    def reset_all(self) -> None:
        """Forget disconnected market state without bypassing global cooldown."""

        self._states = {symbol: _AnchorState() for symbol in self.symbols}
        logger.info("Price anchors cleared after market disconnect")

    def _dispatch_pending(self, now: float) -> None:
        if now < self._cooldown_until:
            return

        selected: tuple[str, _AnchorState] | None = None
        selected_key: tuple[int, float, str] | None = None
        for symbol, state in self._states.items():
            pending_since_ms = state.pending_since_ms
            if pending_since_ms is None or state.current_price is None:
                continue
            key = (pending_since_ms, -state.movement_pct, symbol)
            if selected_key is None or key < selected_key:
                selected = (symbol, state)
                selected_key = key
        if selected is None:
            return

        symbol, state = selected
        assert state.current_price is not None
        alert = Alert(
            symbol=symbol,
            price=state.current_price,
            direction=state.direction,
            movement_pct=state.movement_pct,
        )
        try:
            self.queue.put_nowait(alert)
        except asyncio.QueueFull:
            if not self._queue_blocked:
                logger.error(
                    "Webhook queue is full; alerts remain pending until it drains"
                )
                self._queue_blocked = True
            return

        if self._queue_blocked:
            logger.info("Webhook queue recovered; pending alert queued")
            self._queue_blocked = False

        self._set_anchor(symbol, state, reason="alert triggered")
        self._cooldown_until = now + self.cooldown_seconds
        logger.info(
            "Price alert queued: symbol=%s direction=%s movement=%.2f%%; "
            "all alerts paused for %gs",
            symbol,
            alert.direction,
            alert.movement_pct,
            self.cooldown_seconds,
        )

    @staticmethod
    def _movement(anchor_price: float, current_price: float) -> tuple[str, float]:
        if current_price >= anchor_price:
            return "up", (current_price / anchor_price - 1) * 100
        return "down", (1 - current_price / anchor_price) * 100

    @staticmethod
    def _set_anchor(symbol: str, state: _AnchorState, *, reason: str) -> None:
        assert state.current_price is not None
        previous_price = state.anchor_price
        state.anchor_price = state.current_price
        state.anchor_event_time_ms = state.latest_event_time_ms
        state.pending_since_ms = None
        logger.info(
            "Price anchor updated: symbol=%s previous=%s current=%g reason=%s",
            symbol,
            "none" if previous_price is None else f"{previous_price:g}",
            state.current_price,
            reason,
        )
