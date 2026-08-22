from __future__ import annotations

import json
import unittest
from unittest.mock import Mock, patch

from app.market_stream import (
    BinanceAggTradeReceiver,
    TradeProcessingError,
    _reconnect_delay,
    _silent_symbols,
)


class MarketMessageParsingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.observer = Mock()
        self.receiver = BinanceAggTradeReceiver(
            websocket_url="wss://example.com",
            websocket_proxy=None,
            symbols=("BTCUSDT",),
            observer=self.observer,
        )

    def test_parses_combined_stream_message(self) -> None:
        message = json.dumps(
            {
                "stream": "btcusdt@aggTrade",
                "data": {
                    "e": "aggTrade",
                    "E": 1_700_000_000_000,
                    "s": "BTCUSDT",
                    "p": "65432.10",
                },
            }
        )

        self.assertEqual(
            self.receiver._parse_message(message),
            ("BTCUSDT", 65432.1, 1_700_000_000_000),
        )

    def test_ignores_unknown_symbol_and_non_trade_event(self) -> None:
        unknown = json.dumps(
            {"data": {"e": "aggTrade", "E": 1, "s": "ETHUSDT", "p": "1"}}
        )
        other_event = json.dumps({"data": {"e": "markPriceUpdate"}})

        self.assertIsNone(self.receiver._parse_message(unknown))
        self.assertIsNone(self.receiver._parse_message(other_event))

    def test_ignores_malformed_or_invalid_values(self) -> None:
        self.assertIsNone(self.receiver._parse_message("not-json"))
        self.assertIsNone(
            self.receiver._parse_message(
                json.dumps(
                    {"data": {"e": "aggTrade", "E": 1, "s": "BTCUSDT", "p": "0"}}
                )
            )
        )
        self.assertIsNone(
            self.receiver._parse_message(
                json.dumps(
                    {"data": {"e": "aggTrade", "E": True, "s": "BTCUSDT", "p": "1"}}
                )
            )
        )
        self.assertIsNone(
            self.receiver._parse_message(
                json.dumps(
                    {"data": {"e": "aggTrade", "E": 1, "s": "BTCUSDT", "p": True}}
                )
            )
        )

    def test_reconnect_backoff_is_bounded_for_huge_failure_counts(self) -> None:
        self.assertEqual(
            [_reconnect_delay(failures) for failures in range(7)],
            [1, 2, 4, 8, 16, 30, 30],
        )
        self.assertEqual(_reconnect_delay(1_000_000), 30)

    def test_silent_symbol_detection_uses_strict_timeout_boundary(self) -> None:
        last_valid_at = {"BTCUSDT": 90, "ETHUSDT": 90.1}

        self.assertEqual(_silent_symbols(last_valid_at, 100), [])
        self.assertEqual(_silent_symbols(last_valid_at, 100.1), ["BTCUSDT"])

    def test_accepted_trade_is_forwarded_immediately_to_observer(self) -> None:
        accepted = self.receiver._record_trade("BTCUSDT", 20_400, 60_000)

        self.assertTrue(accepted)
        self.observer.observe.assert_called_once_with(
            "BTCUSDT", 20_400, 60_000
        )
        state = self.receiver._states["BTCUSDT"]
        self.assertEqual(state.current_price, 20_400)
        self.assertEqual(state.trade_count, 1)

    def test_out_of_order_trade_is_not_forwarded(self) -> None:
        self.receiver._record_trade("BTCUSDT", 20_400, 60_000)
        self.observer.reset_mock()

        accepted = self.receiver._record_trade("BTCUSDT", 20_000, 59_999)

        self.assertFalse(accepted)
        self.observer.observe.assert_not_called()

    def test_observer_failure_is_fatal_instead_of_silently_reconnecting(self) -> None:
        self.observer.observe.side_effect = ValueError("boom")

        with self.assertRaises(TradeProcessingError):
            self.receiver._record_trade("BTCUSDT", 20_400, 60_000)

        state = self.receiver._states["BTCUSDT"]
        self.assertIsNone(state.current_price)
        self.assertEqual(state.trade_count, 0)

    def test_reset_allows_fresh_lower_event_time_after_reconnect(self) -> None:
        self.receiver._record_trade("BTCUSDT", 20_400, 60_000)

        self.receiver._reset_market_state()
        accepted = self.receiver._record_trade("BTCUSDT", 20_000, 50_000)

        self.assertTrue(accepted)
        state = self.receiver._states["BTCUSDT"]
        self.assertEqual(state.current_price, 20_000)
        self.assertEqual(state.trade_count, 1)

    def test_health_log_reports_constant_state_trade_count(self) -> None:
        self.receiver._record_trade("BTCUSDT", 20_400, 60_000)
        self.receiver._record_trade("BTCUSDT", 20_401, 60_001)

        with patch("app.market_stream.time.monotonic", return_value=100):
            with self.assertLogs("app.market_stream", level="INFO") as logs:
                self.receiver._log_status(90, {"BTCUSDT": 99})

        message = logs.records[0].getMessage()
        self.assertIn("connected=10s", message)
        self.assertIn("BTCUSDT=20401(trades=2,age=1.0s)", message)


if __name__ == "__main__":
    unittest.main()
