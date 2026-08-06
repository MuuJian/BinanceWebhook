from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from app.alert_engine import AlertEngine
from app.price_window import WindowSnapshot


class _SnapshotStore:
    def __init__(self, snapshot: WindowSnapshot) -> None:
        self.snapshot_value = snapshot

    def snapshot(self, symbol: str) -> WindowSnapshot:
        return self.snapshot_value


def _down_snapshot(movement_pct: float) -> WindowSnapshot:
    high = 100.0
    current = high * (1 - movement_pct / 100)
    return WindowSnapshot(
        generation=0,
        current_price=current,
        highest_price=high,
        lowest_price=current,
        high_event_time_ms=1_000,
        low_event_time_ms=2_000,
        latest_event_time_ms=2_000,
        trade_count=20,
        bucket_count=20,
        span_seconds=60,
    )


class AlertEngineStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.queue: asyncio.Queue = asyncio.Queue()
        self.store = _SnapshotStore(_down_snapshot(0))
        self.engine = AlertEngine(
            symbols=("BTCUSDT",),
            store=self.store,
            queue=self.queue,
            threshold_pct=3,
            reset_pct=2,
            reset_confirm_seconds=10,
            cooldown_seconds=30,
            evaluation_interval_seconds=1,
            min_points=20,
            warmup_seconds=60,
            window_seconds=120,
        )

    def evaluate(self, movement_pct: float, now: float) -> None:
        self.store.snapshot_value = _down_snapshot(movement_pct)
        with patch("app.alert_engine.time.monotonic", return_value=now):
            self.engine.evaluate_once()

    def tiers(self) -> list[int]:
        return [alert.tier for alert in self.queue._queue]

    def test_same_tier_never_repeats_after_cooldown(self) -> None:
        self.evaluate(3.2, 0)
        self.evaluate(3.2, 31)
        self.evaluate(5.9, 100)
        self.assertEqual(self.tiers(), [1])

    def test_three_pct_boundary_jitter_does_not_reset(self) -> None:
        self.evaluate(3.2, 0)
        self.evaluate(2.99, 31)
        self.evaluate(3.01, 32)
        self.evaluate(2.5, 100)
        self.evaluate(3.2, 101)
        self.assertEqual(self.tiers(), [1])

    def test_direct_jump_is_sent_one_tier_per_cooldown(self) -> None:
        self.evaluate(10, 0)
        self.evaluate(10, 29)
        self.evaluate(10, 30)
        self.evaluate(10, 60)
        self.assertEqual(self.tiers(), [1, 2, 3])

    def test_pending_tier_is_rechecked_before_send(self) -> None:
        self.evaluate(10, 0)
        self.evaluate(5.9, 30)
        self.assertEqual(self.tiers(), [1])
        self.evaluate(6.1, 31)
        self.assertEqual(self.tiers(), [1, 2])

    def test_reset_requires_ten_continuous_seconds_below_two_pct(self) -> None:
        self.evaluate(3.2, 0)
        self.evaluate(1.9, 31)
        self.evaluate(1.9, 40)
        self.evaluate(3.2, 41)
        self.assertEqual(self.tiers(), [1])

        self.evaluate(1.9, 42)
        self.evaluate(1.9, 52)
        self.evaluate(3.2, 53)
        self.assertEqual(self.tiers(), [1, 1])

    def test_reset_does_not_bypass_hard_cooldown(self) -> None:
        self.evaluate(3.2, 0)
        self.evaluate(1.9, 5)
        self.evaluate(1.9, 15)
        self.evaluate(3.2, 16)
        self.assertEqual(self.tiers(), [1])
        self.evaluate(3.2, 30)
        self.assertEqual(self.tiers(), [1, 1])


if __name__ == "__main__":
    unittest.main()
