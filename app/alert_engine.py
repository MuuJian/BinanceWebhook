"""Evaluate movement from fixed per-symbol anchors with a global cooldown."""

from __future__ import annotations

import asyncio
import logging
import math
import time
from dataclasses import dataclass
from typing import Protocol

from app.price_window import LatestPriceSnapshot

logger = logging.getLogger(__name__)


class PriceSource(Protocol):
    def latest(self, symbol: str) -> LatestPriceSnapshot | None: ...


@dataclass(frozen=True, slots=True)
class Alert:
    symbol: str
    price: float
    direction: str
    movement_pct: float


@dataclass(slots=True)
class _AnchorState:
    generation: int = -1
    price: float | None = None
    event_time_ms: int = 0
    pending_since_ms: int | None = None


@dataclass(frozen=True, slots=True)
class _Candidate:
    symbol: str
    snapshot: LatestPriceSnapshot
    direction: str
    movement_pct: float
    pending_since_ms: int


class AlertEngine:
    def __init__(
        self,
        *,
        symbols: tuple[str, ...],
        store: PriceSource,
        queue: asyncio.Queue[Alert],
        threshold_pct: float,
        cooldown_seconds: float,
        evaluation_interval_seconds: float,
        window_seconds: int,
    ) -> None:
        self.symbols = symbols
        self.store = store
        self.queue = queue
        self.threshold_pct = threshold_pct
        self.cooldown_seconds = cooldown_seconds
        self.evaluation_interval_seconds = evaluation_interval_seconds
        self.window_ms = window_seconds * 1000
        self._cooldown_until = -math.inf
        self._states = {symbol: _AnchorState() for symbol in symbols}

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
        candidates: list[_Candidate] = []

        for symbol in self.symbols:
            snapshot = self.store.latest(symbol)
            if snapshot is None:
                continue

            state = self._states[symbol]
            if state.generation != snapshot.generation or state.price is None:
                self._set_anchor(symbol, state, snapshot, reason="initialized")
                continue

            direction, movement = self._movement(state.price, snapshot.current_price)
            if movement >= self.threshold_pct:
                if state.pending_since_ms is None:
                    state.pending_since_ms = snapshot.latest_event_time_ms
                candidates.append(
                    _Candidate(
                        symbol,
                        snapshot,
                        direction,
                        movement,
                        state.pending_since_ms,
                    )
                )
                continue

            state.pending_since_ms = None
            if snapshot.latest_event_time_ms - state.event_time_ms >= self.window_ms:
                self._set_anchor(symbol, state, snapshot, reason="window expired")

        # Anchor maintenance continues during cooldown, but calls stay paused.
        if now < self._cooldown_until or not candidates:
            return

        candidate = min(
            candidates,
            key=lambda item: (
                item.pending_since_ms,
                -item.movement_pct,
                item.symbol,
            ),
        )
        alert = Alert(
            symbol=candidate.symbol,
            price=candidate.snapshot.current_price,
            direction=candidate.direction,
            movement_pct=candidate.movement_pct,
        )
        try:
            self.queue.put_nowait(alert)
        except asyncio.QueueFull:
            logger.error(
                "Webhook queue is full; alert dropped for %s", candidate.symbol
            )
            return

        self._set_anchor(
            candidate.symbol,
            self._states[candidate.symbol],
            candidate.snapshot,
            reason="alert triggered",
        )
        self._cooldown_until = now + self.cooldown_seconds
        logger.info(
            "Price alert queued: symbol=%s direction=%s movement=%.2f%%; "
            "all alerts paused for %gs",
            candidate.symbol,
            candidate.direction,
            candidate.movement_pct,
            self.cooldown_seconds,
        )

    @staticmethod
    def _movement(anchor_price: float, current_price: float) -> tuple[str, float]:
        if current_price >= anchor_price:
            return "up", (current_price / anchor_price - 1) * 100
        return "down", (1 - current_price / anchor_price) * 100

    @staticmethod
    def _set_anchor(
        symbol: str,
        state: _AnchorState,
        snapshot: LatestPriceSnapshot,
        *,
        reason: str,
    ) -> None:
        previous_price = state.price
        state.generation = snapshot.generation
        state.price = snapshot.current_price
        state.event_time_ms = snapshot.latest_event_time_ms
        state.pending_since_ms = None
        logger.info(
            "Price anchor updated: symbol=%s previous=%s current=%g reason=%s",
            symbol,
            "none" if previous_price is None else f"{previous_price:g}",
            snapshot.current_price,
            reason,
        )
