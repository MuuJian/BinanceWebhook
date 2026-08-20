from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from app.alert_engine import Alert, AlertEngine
from app.price_window import WindowSnapshot


def _snapshot(
    *,
    current: float,
    high: float,
    low: float,
    high_time_ms: int = 1_000,
    low_time_ms: int = 1_000,
    trade_count: int = 20,
    span_seconds: float = 60,
) -> WindowSnapshot:
    return WindowSnapshot(
        generation=0,
        current_price=current,
        highest_price=high,
        lowest_price=low,
        high_event_time_ms=high_time_ms,
        low_event_time_ms=low_time_ms,
        latest_event_time_ms=2_000,
        trade_count=trade_count,
        bucket_count=20,
        span_seconds=span_seconds,
    )


def _down(movement_pct: float) -> WindowSnapshot:
    high = 100.0
    return _snapshot(
        current=high * (1 - movement_pct / 100),
        high=high,
        low=high * (1 - movement_pct / 100),
        high_time_ms=1_000,
        low_time_ms=2_000,
    )


def _up(movement_pct: float) -> WindowSnapshot:
    low = 100.0
    return _snapshot(
        current=low * (1 + movement_pct / 100),
        high=low * (1 + movement_pct / 100),
        low=low,
        high_time_ms=2_000,
        low_time_ms=1_000,
    )


class _SnapshotStore:
    def __init__(self, snapshots: dict[str, WindowSnapshot]) -> None:
        self.snapshots = snapshots

    def snapshot(self, symbol: str) -> WindowSnapshot | None:
        return self.snapshots.get(symbol)


class AlertEngineCooldownTests(unittest.TestCase):
    def setUp(self) -> None:
        self.queue: asyncio.Queue[Alert] = asyncio.Queue()
        self.store = _SnapshotStore(
            {"BTCUSDT": _down(0), "ETHUSDT": _down(0)}
        )
        self.engine = AlertEngine(
            symbols=("BTCUSDT", "ETHUSDT"),
            store=self.store,
            queue=self.queue,
            threshold_pct=3,
            cooldown_seconds=30,
            evaluation_interval_seconds=1,
            min_points=20,
            warmup_seconds=60,
        )

    def evaluate(self, now: float) -> None:
        with patch("app.alert_engine.time.monotonic", return_value=now):
            self.engine.evaluate_once()

    def alerts(self) -> list[Alert]:
        return list(self.queue._queue)

    def test_below_three_percent_does_not_alert(self) -> None:
        self.store.snapshots["BTCUSDT"] = _down(2.99)

        self.evaluate(0)

        self.assertEqual(self.alerts(), [])

    def test_exactly_three_percent_triggers_alert(self) -> None:
        self.store.snapshots["BTCUSDT"] = _down(3)

        self.evaluate(0)

        self.assertEqual(len(self.alerts()), 1)

    def test_same_three_percent_move_can_alert_again_after_thirty_seconds(self) -> None:
        self.store.snapshots["BTCUSDT"] = _down(3.2)

        self.evaluate(0)
        self.evaluate(29.999)
        self.evaluate(30)

        self.assertEqual(
            [alert.symbol for alert in self.alerts()],
            ["BTCUSDT", "BTCUSDT"],
        )

    def test_cooldown_suppresses_all_symbols_and_directions(self) -> None:
        self.store.snapshots["BTCUSDT"] = _down(3.2)
        self.evaluate(0)

        self.store.snapshots["BTCUSDT"] = _down(0)
        self.store.snapshots["ETHUSDT"] = _up(4)
        self.evaluate(10)
        self.evaluate(30)

        self.assertEqual(
            [(alert.symbol, alert.direction) for alert in self.alerts()],
            [("BTCUSDT", "down"), ("ETHUSDT", "up")],
        )

    def test_latest_price_collected_during_cooldown_is_used_after_wakeup(self) -> None:
        self.store.snapshots["BTCUSDT"] = _down(3.2)
        self.evaluate(0)

        # Market data collection is independent of alert evaluation, so the
        # latest rolling snapshot can keep changing during the cooldown.
        self.store.snapshots["BTCUSDT"] = _up(4.5)
        self.evaluate(15)
        self.evaluate(30)

        alerts = self.alerts()
        self.assertEqual(len(alerts), 2)
        self.assertEqual(alerts[1].direction, "up")
        self.assertEqual(alerts[1].price, 104.5)

    def test_not_enough_history_does_not_alert(self) -> None:
        self.store.snapshots["BTCUSDT"] = _snapshot(
            current=104,
            high=104,
            low=100,
            trade_count=19,
            span_seconds=59,
        )

        self.evaluate(0)

        self.assertEqual(self.alerts(), [])

    def test_more_recent_extreme_selects_direction_when_both_qualify(self) -> None:
        self.store.snapshots["BTCUSDT"] = _snapshot(
            current=100,
            high=105,
            low=95,
            high_time_ms=2_000,
            low_time_ms=1_000,
        )

        self.evaluate(0)

        self.assertEqual(self.alerts()[0].direction, "down")

    def test_most_recent_qualifying_move_wins_across_symbols(self) -> None:
        self.store.snapshots["BTCUSDT"] = _snapshot(
            current=104,
            high=104,
            low=100,
            low_time_ms=1_000,
        )
        self.store.snapshots["ETHUSDT"] = _snapshot(
            current=96,
            high=100,
            low=96,
            high_time_ms=2_000,
        )

        self.evaluate(0)

        self.assertEqual(self.alerts()[0].symbol, "ETHUSDT")


if __name__ == "__main__":
    unittest.main()
