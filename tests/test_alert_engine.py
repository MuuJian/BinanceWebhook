from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from app.alert_engine import Alert, AlertEngine
from app.price_window import LatestPriceSnapshot


def _snapshot(
    current: float,
    event_time_ms: int,
    *,
    generation: int = 0,
) -> LatestPriceSnapshot:
    return LatestPriceSnapshot(
        generation=generation,
        current_price=current,
        latest_event_time_ms=event_time_ms,
    )


class _SnapshotStore:
    def __init__(self, snapshots: dict[str, LatestPriceSnapshot]) -> None:
        self.snapshots = snapshots

    def latest(self, symbol: str) -> LatestPriceSnapshot | None:
        return self.snapshots.get(symbol)


class AlertEngineAnchorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.queue: asyncio.Queue[Alert] = asyncio.Queue()
        self.store = _SnapshotStore(
            {
                "BTCUSDT": _snapshot(20_000, 0),
                "ETHUSDT": _snapshot(2_000, 0),
            }
        )
        self.engine = AlertEngine(
            symbols=("BTCUSDT", "ETHUSDT"),
            store=self.store,
            queue=self.queue,
            threshold_pct=2,
            cooldown_seconds=30,
            evaluation_interval_seconds=1,
            window_seconds=300,
        )

    def evaluate(self, now: float) -> None:
        with patch("app.alert_engine.time.monotonic", return_value=now):
            self.engine.evaluate_once()

    def set_price(
        self,
        symbol: str,
        price: float,
        event_time_ms: int,
        *,
        generation: int = 0,
    ) -> None:
        self.store.snapshots[symbol] = _snapshot(
            price, event_time_ms, generation=generation
        )

    def alerts(self) -> list[Alert]:
        return list(self.queue._queue)

    def initialize_anchors(self) -> None:
        self.evaluate(0)

    def test_first_ready_price_only_initializes_anchor(self) -> None:
        self.evaluate(0)

        self.assertEqual(self.alerts(), [])

    def test_two_percent_move_alerts_and_becomes_new_anchor(self) -> None:
        self.initialize_anchors()
        self.set_price("BTCUSDT", 20_400, 60_000)

        self.evaluate(60)
        self.evaluate(90)

        self.assertEqual(len(self.alerts()), 1)
        self.assertEqual(self.alerts()[0].price, 20_400)

    def test_next_two_percent_is_measured_from_trigger_price(self) -> None:
        self.initialize_anchors()
        self.set_price("BTCUSDT", 20_400, 60_000)
        self.evaluate(60)

        self.set_price("BTCUSDT", 20_808, 90_000)
        self.evaluate(90)

        self.assertEqual([alert.price for alert in self.alerts()], [20_400, 20_808])

    def test_two_percent_drop_from_anchor_triggers_down_alert(self) -> None:
        self.initialize_anchors()
        self.set_price("BTCUSDT", 19_600, 60_000)

        self.evaluate(60)

        self.assertEqual(len(self.alerts()), 1)
        self.assertEqual(self.alerts()[0].direction, "down")
        self.assertAlmostEqual(self.alerts()[0].movement_pct, 2)

    def test_anchor_stays_fixed_before_window_expires(self) -> None:
        self.initialize_anchors()
        self.set_price("BTCUSDT", 20_100, 100_000)
        self.evaluate(100)
        self.set_price("BTCUSDT", 20_400, 299_999)

        self.evaluate(299.999)

        self.assertEqual([alert.price for alert in self.alerts()], [20_400])

    def test_quiet_window_refreshes_anchor_to_latest_price(self) -> None:
        self.initialize_anchors()
        self.set_price("BTCUSDT", 20_100, 300_000)
        self.evaluate(300)

        self.set_price("BTCUSDT", 20_501, 301_000)
        self.evaluate(301)
        self.assertEqual(self.alerts(), [])

        self.set_price("BTCUSDT", 20_502, 302_000)
        self.evaluate(302)
        self.assertEqual([alert.price for alert in self.alerts()], [20_502])

    def test_global_cooldown_suppresses_other_symbol_but_keeps_its_anchor(self) -> None:
        self.initialize_anchors()
        self.set_price("BTCUSDT", 20_400, 60_000)
        self.evaluate(60)

        self.set_price("ETHUSDT", 2_040, 70_000)
        self.evaluate(70)
        self.evaluate(90)

        self.assertEqual(
            [alert.symbol for alert in self.alerts()],
            ["BTCUSDT", "ETHUSDT"],
        )

    def test_quiet_anchor_refresh_continues_during_global_cooldown(self) -> None:
        self.initialize_anchors()
        self.set_price("BTCUSDT", 20_400, 300_000)
        self.evaluate(10)

        self.set_price("ETHUSDT", 2_010, 300_000)
        self.evaluate(20)
        self.set_price("ETHUSDT", 2_050, 301_000)
        self.evaluate(40)

        self.assertEqual([alert.symbol for alert in self.alerts()], ["BTCUSDT"])

    def test_reconnect_generation_resets_anchor_without_alerting(self) -> None:
        self.initialize_anchors()
        self.set_price("BTCUSDT", 30_000, 60_000, generation=1)

        self.evaluate(60)

        self.assertEqual(self.alerts(), [])

    def test_first_trade_can_be_anchor_for_first_minute_move(self) -> None:
        self.store.snapshots["BTCUSDT"] = _snapshot(20_000, 0)
        self.evaluate(0)
        self.store.snapshots["BTCUSDT"] = _snapshot(20_400, 60_000)

        self.evaluate(60)

        self.assertEqual([alert.price for alert in self.alerts()], [20_400])

    def test_oldest_pending_symbol_is_not_starved(self) -> None:
        self.initialize_anchors()
        self.set_price("BTCUSDT", 20_400, 100_000)
        self.set_price("ETHUSDT", 2_040, 90_000)

        self.evaluate(100)
        self.evaluate(130)

        self.assertEqual(
            [alert.symbol for alert in self.alerts()],
            ["ETHUSDT", "BTCUSDT"],
        )

    def test_pending_move_clears_if_price_returns_during_cooldown(self) -> None:
        self.initialize_anchors()
        self.set_price("BTCUSDT", 20_400, 60_000)
        self.evaluate(60)

        self.set_price("ETHUSDT", 2_040, 70_000)
        self.evaluate(70)
        self.set_price("ETHUSDT", 2_010, 80_000)
        self.evaluate(80)
        self.evaluate(90)

        self.assertEqual([alert.symbol for alert in self.alerts()], ["BTCUSDT"])


if __name__ == "__main__":
    unittest.main()
