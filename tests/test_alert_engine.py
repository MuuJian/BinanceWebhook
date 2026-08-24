from __future__ import annotations

import asyncio
import unittest

from app.alert_engine import Alert, AlertEngine


class AlertEngineAnchorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.queue: asyncio.Queue[Alert] = asyncio.Queue()
        self.engine = AlertEngine(
            symbols=("BTCUSDT", "ETHUSDT"),
            queue=self.queue,
            threshold_pct=2,
            window_seconds=300,
        )

    def observe(self, symbol: str, price: float, event_time_ms: int) -> None:
        self.engine.observe(symbol, price, event_time_ms)

    def alerts(self) -> list[Alert]:
        return list(self.queue._queue)

    def initialize_anchors(self) -> None:
        self.observe("BTCUSDT", 20_000, 0)
        self.observe("ETHUSDT", 2_000, 0)

    def test_first_trade_only_initializes_anchor(self) -> None:
        self.observe("BTCUSDT", 20_000, 0)

        self.assertEqual(self.alerts(), [])

    def test_two_percent_move_alerts_and_becomes_new_anchor(self) -> None:
        self.initialize_anchors()

        self.observe("BTCUSDT", 20_400, 60_000)
        self.observe("BTCUSDT", 20_400, 90_000)

        self.assertEqual([alert.price for alert in self.alerts()], [20_400])

    def test_next_two_percent_alerts_immediately_from_trigger_price(self) -> None:
        self.initialize_anchors()
        self.observe("BTCUSDT", 20_400, 60_000)

        self.observe("BTCUSDT", 20_808, 60_100)

        self.assertEqual([alert.price for alert in self.alerts()], [20_400, 20_808])

    def test_two_percent_drop_from_anchor_triggers_down_alert(self) -> None:
        self.initialize_anchors()

        self.observe("BTCUSDT", 19_600, 60_000)

        self.assertEqual(len(self.alerts()), 1)
        self.assertEqual(self.alerts()[0].direction, "down")
        self.assertAlmostEqual(self.alerts()[0].movement_pct, 2)

    def test_transient_threshold_crossing_cannot_be_missed(self) -> None:
        self.initialize_anchors()

        self.observe("BTCUSDT", 20_400, 60_000)
        self.observe("BTCUSDT", 20_000, 60_100)

        self.assertEqual([alert.price for alert in self.alerts()], [20_400])

    def test_anchor_stays_fixed_before_window_expires(self) -> None:
        self.initialize_anchors()
        self.observe("BTCUSDT", 20_100, 100_000)

        self.observe("BTCUSDT", 20_400, 299_999)

        self.assertEqual([alert.price for alert in self.alerts()], [20_400])

    def test_quiet_window_refreshes_anchor_to_latest_price(self) -> None:
        self.initialize_anchors()
        self.observe("BTCUSDT", 20_100, 300_000)

        self.observe("BTCUSDT", 20_501, 301_000)
        self.assertEqual(self.alerts(), [])
        self.observe("BTCUSDT", 20_502, 302_000)

        self.assertEqual([alert.price for alert in self.alerts()], [20_502])

    def test_alert_restarts_window_before_quiet_anchor_refresh(self) -> None:
        self.initialize_anchors()
        self.observe("BTCUSDT", 20_400, 60_000)

        self.observe("BTCUSDT", 20_500, 359_999)
        self.observe("BTCUSDT", 20_500, 360_000)
        self.observe("BTCUSDT", 20_808, 361_000)
        self.assertEqual([alert.price for alert in self.alerts()], [20_400])

        self.observe("BTCUSDT", 20_910, 362_000)
        self.assertEqual([alert.price for alert in self.alerts()], [20_400, 20_910])

    def test_other_symbol_alerts_immediately_without_global_pause(self) -> None:
        self.initialize_anchors()
        self.observe("BTCUSDT", 20_400, 60_000)

        self.observe("ETHUSDT", 2_040, 60_100)

        self.assertEqual(
            [alert.symbol for alert in self.alerts()],
            ["BTCUSDT", "ETHUSDT"],
        )

    def test_reset_clears_anchor_without_alerting(self) -> None:
        self.initialize_anchors()
        self.engine.reset_all()

        self.observe("BTCUSDT", 30_000, 60_000)

        self.assertEqual(self.alerts(), [])

    def test_oldest_pending_symbol_is_not_starved_by_full_queue(self) -> None:
        queue: asyncio.Queue[Alert] = asyncio.Queue(maxsize=1)
        queue.put_nowait(Alert("SOLUSDT", 100, "up", 2))
        engine = AlertEngine(
            symbols=("BTCUSDT", "ETHUSDT"),
            queue=queue,
            threshold_pct=2,
            window_seconds=300,
        )
        engine.observe("BTCUSDT", 20_000, 0)
        engine.observe("ETHUSDT", 2_000, 0)
        engine.observe("BTCUSDT", 20_400, 10_000)
        engine.observe("ETHUSDT", 2_040, 20_000)

        queue.get_nowait()
        queue.task_done()
        engine.observe("ETHUSDT", 2_040, 30_000)
        first = queue.get_nowait()
        queue.task_done()
        engine.observe("ETHUSDT", 2_040, 40_000)
        second = queue.get_nowait()

        self.assertEqual([first.symbol, second.symbol], ["BTCUSDT", "ETHUSDT"])

    def test_pending_move_clears_if_price_returns_while_queue_is_full(self) -> None:
        queue: asyncio.Queue[Alert] = asyncio.Queue(maxsize=1)
        queue.put_nowait(Alert("ETHUSDT", 2_000, "up", 2))
        engine = AlertEngine(
            symbols=("BTCUSDT",),
            queue=queue,
            threshold_pct=2,
            window_seconds=300,
        )
        engine.observe("BTCUSDT", 20_000, 0)
        engine.observe("BTCUSDT", 20_400, 1_000)
        engine.observe("BTCUSDT", 20_100, 2_000)

        queue.get_nowait()
        queue.task_done()
        engine.observe("BTCUSDT", 20_100, 3_000)

        self.assertTrue(queue.empty())

    def test_full_queue_logs_once_and_retries_pending_alert(self) -> None:
        queue: asyncio.Queue[Alert] = asyncio.Queue(maxsize=1)
        queue.put_nowait(Alert("ETHUSDT", 2_000, "up", 2))
        engine = AlertEngine(
            symbols=("BTCUSDT",),
            queue=queue,
            threshold_pct=2,
            window_seconds=300,
        )

        engine.observe("BTCUSDT", 20_000, 0)
        with self.assertLogs("app.alert_engine", level="ERROR") as logs:
            engine.observe("BTCUSDT", 20_400, 1_000)
            engine.observe("BTCUSDT", 20_500, 1_100)

        self.assertEqual(len(logs.records), 1)
        queue.get_nowait()
        queue.task_done()
        engine.observe("BTCUSDT", 20_500, 2_000)

        queued = queue.get_nowait()
        self.assertEqual(queued.symbol, "BTCUSDT")
        self.assertEqual(queued.price, 20_500)


if __name__ == "__main__":
    unittest.main()
