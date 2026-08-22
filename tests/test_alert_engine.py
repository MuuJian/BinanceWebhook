from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from app.alert_engine import Alert, AlertEngine


class AlertEngineAnchorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.queue: asyncio.Queue[Alert] = asyncio.Queue()
        self.engine = AlertEngine(
            symbols=("BTCUSDT", "ETHUSDT"),
            queue=self.queue,
            threshold_pct=2,
            cooldown_seconds=30,
            window_seconds=300,
        )

    def observe(
        self,
        symbol: str,
        price: float,
        event_time_ms: int,
        now: float,
    ) -> None:
        with patch("app.alert_engine.time.monotonic", return_value=now):
            self.engine.observe(symbol, price, event_time_ms)

    def alerts(self) -> list[Alert]:
        return list(self.queue._queue)

    def initialize_anchors(self) -> None:
        self.observe("BTCUSDT", 20_000, 0, 0)
        self.observe("ETHUSDT", 2_000, 0, 0)

    def test_first_trade_only_initializes_anchor(self) -> None:
        self.observe("BTCUSDT", 20_000, 0, 0)

        self.assertEqual(self.alerts(), [])

    def test_two_percent_move_alerts_and_becomes_new_anchor(self) -> None:
        self.initialize_anchors()

        self.observe("BTCUSDT", 20_400, 60_000, 60)
        self.observe("BTCUSDT", 20_400, 90_000, 90)

        self.assertEqual([alert.price for alert in self.alerts()], [20_400])

    def test_next_two_percent_is_measured_from_trigger_price(self) -> None:
        self.initialize_anchors()
        self.observe("BTCUSDT", 20_400, 60_000, 60)

        self.observe("BTCUSDT", 20_808, 90_000, 90)

        self.assertEqual([alert.price for alert in self.alerts()], [20_400, 20_808])

    def test_two_percent_drop_from_anchor_triggers_down_alert(self) -> None:
        self.initialize_anchors()

        self.observe("BTCUSDT", 19_600, 60_000, 60)

        self.assertEqual(len(self.alerts()), 1)
        self.assertEqual(self.alerts()[0].direction, "down")
        self.assertAlmostEqual(self.alerts()[0].movement_pct, 2)

    def test_transient_threshold_crossing_cannot_be_missed(self) -> None:
        self.initialize_anchors()

        self.observe("BTCUSDT", 20_400, 60_000, 60)
        self.observe("BTCUSDT", 20_000, 60_100, 60.1)

        self.assertEqual([alert.price for alert in self.alerts()], [20_400])

    def test_anchor_stays_fixed_before_window_expires(self) -> None:
        self.initialize_anchors()
        self.observe("BTCUSDT", 20_100, 100_000, 100)

        self.observe("BTCUSDT", 20_400, 299_999, 299.999)

        self.assertEqual([alert.price for alert in self.alerts()], [20_400])

    def test_quiet_window_refreshes_anchor_to_latest_price(self) -> None:
        self.initialize_anchors()
        self.observe("BTCUSDT", 20_100, 300_000, 300)

        self.observe("BTCUSDT", 20_501, 301_000, 301)
        self.assertEqual(self.alerts(), [])
        self.observe("BTCUSDT", 20_502, 302_000, 302)

        self.assertEqual([alert.price for alert in self.alerts()], [20_502])

    def test_global_cooldown_defers_other_symbol_with_fixed_anchor(self) -> None:
        self.initialize_anchors()
        self.observe("BTCUSDT", 20_400, 60_000, 60)

        self.observe("ETHUSDT", 2_040, 70_000, 70)
        self.observe("BTCUSDT", 20_400, 90_000, 90)

        self.assertEqual(
            [alert.symbol for alert in self.alerts()],
            ["BTCUSDT", "ETHUSDT"],
        )

    def test_quiet_anchor_refresh_continues_during_global_cooldown(self) -> None:
        self.initialize_anchors()
        self.observe("BTCUSDT", 20_400, 300_000, 10)

        self.observe("ETHUSDT", 2_010, 300_000, 20)
        self.observe("ETHUSDT", 2_050, 301_000, 40)

        self.assertEqual([alert.symbol for alert in self.alerts()], ["BTCUSDT"])

    def test_reset_clears_anchor_without_alerting(self) -> None:
        self.initialize_anchors()
        self.engine.reset_all()

        self.observe("BTCUSDT", 30_000, 60_000, 60)

        self.assertEqual(self.alerts(), [])

    def test_reset_does_not_bypass_global_cooldown(self) -> None:
        self.initialize_anchors()
        self.observe("BTCUSDT", 20_400, 10_000, 0)
        self.engine.reset_all()
        self.observe("ETHUSDT", 2_000, 11_000, 10)

        self.observe("ETHUSDT", 2_040, 20_000, 20)
        self.assertEqual([alert.symbol for alert in self.alerts()], ["BTCUSDT"])
        self.observe("ETHUSDT", 2_040, 30_000, 30)

        self.assertEqual(
            [alert.symbol for alert in self.alerts()],
            ["BTCUSDT", "ETHUSDT"],
        )

    def test_oldest_pending_symbol_is_not_starved(self) -> None:
        self.initialize_anchors()
        self.observe("BTCUSDT", 20_400, 10_000, 10)
        self.observe("ETHUSDT", 2_040, 20_000, 20)
        self.observe("BTCUSDT", 20_808, 30_000, 30)

        self.observe("BTCUSDT", 20_808, 40_000, 40)
        self.observe("BTCUSDT", 20_808, 70_000, 70)

        self.assertEqual(
            [alert.symbol for alert in self.alerts()],
            ["BTCUSDT", "ETHUSDT", "BTCUSDT"],
        )

    def test_pending_move_clears_if_price_returns_during_cooldown(self) -> None:
        self.initialize_anchors()
        self.observe("BTCUSDT", 20_400, 60_000, 60)

        self.observe("ETHUSDT", 2_040, 70_000, 70)
        self.observe("ETHUSDT", 2_010, 80_000, 80)
        self.observe("BTCUSDT", 20_400, 90_000, 90)

        self.assertEqual([alert.symbol for alert in self.alerts()], ["BTCUSDT"])


if __name__ == "__main__":
    unittest.main()
