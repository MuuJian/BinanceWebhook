from __future__ import annotations

import unittest

from app.price_window import PriceWindow, PriceWindowStore


class PriceWindowTests(unittest.TestCase):
    def test_compacts_trades_and_preserves_extremes(self) -> None:
        window = PriceWindow(window_seconds=2)

        self.assertTrue(window.update(100, 1_000))
        self.assertTrue(window.update(105, 1_100))
        self.assertTrue(window.update(95, 1_900))
        self.assertTrue(window.update(101, 2_000))

        snapshot = window.snapshot()
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.current_price, 101)
        self.assertEqual(snapshot.highest_price, 105)
        self.assertEqual(snapshot.lowest_price, 95)
        self.assertEqual(snapshot.high_event_time_ms, 1_100)
        self.assertEqual(snapshot.low_event_time_ms, 1_900)
        self.assertEqual(snapshot.trade_count, 4)
        self.assertEqual(snapshot.bucket_count, 2)

    def test_evicts_the_expired_second(self) -> None:
        window = PriceWindow(window_seconds=2)
        window.update(90, 1_999)
        window.update(100, 2_000)
        window.update(110, 3_000)

        snapshot = window.snapshot()
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.lowest_price, 100)
        self.assertEqual(snapshot.trade_count, 2)

    def test_rejects_out_of_order_trade_without_mutating_window(self) -> None:
        window = PriceWindow(window_seconds=10)
        window.update(100, 2_000)

        self.assertFalse(window.update(1, 1_999))
        snapshot = window.snapshot()
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.current_price, 100)
        self.assertEqual(snapshot.trade_count, 1)

    def test_latest_snapshot_is_lightweight_and_tracks_last_trade(self) -> None:
        window = PriceWindow(window_seconds=10)
        window.update(100, 1_000)
        window.update(101, 1_100)

        latest = window.latest()

        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest.current_price, 101)
        self.assertEqual(latest.latest_event_time_ms, 1_100)

    def test_clear_increments_generation(self) -> None:
        store = PriceWindowStore(("BTCUSDT",), window_seconds=10)
        store.update("BTCUSDT", 100, 1_000)
        before = store.snapshot("BTCUSDT")
        self.assertIsNotNone(before)

        store.clear_all()
        self.assertIsNone(store.snapshot("BTCUSDT"))
        self.assertIsNone(store.latest("BTCUSDT"))
        store.update("BTCUSDT", 101, 2_000)
        after = store.snapshot("BTCUSDT")
        latest = store.latest("BTCUSDT")
        self.assertIsNotNone(after)
        self.assertIsNotNone(latest)
        assert before is not None and after is not None and latest is not None
        self.assertEqual(after.generation, before.generation + 1)
        self.assertEqual(latest.generation, after.generation)


if __name__ == "__main__":
    unittest.main()
