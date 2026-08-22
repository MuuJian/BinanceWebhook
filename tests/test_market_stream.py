from __future__ import annotations

import json
import unittest
from unittest.mock import Mock

from app.market_stream import (
    BinanceAggTradeReceiver,
    TradeProcessingError,
    _reconnect_delay,
)


class MarketMessageParsingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Mock()
        self.observer = Mock()
        self.receiver = BinanceAggTradeReceiver(
            websocket_url="wss://example.com",
            websocket_proxy=None,
            symbols=("BTCUSDT",),
            store=self.store,
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

    def test_accepted_trade_is_forwarded_immediately_to_observer(self) -> None:
        self.store.update.return_value = True

        accepted = self.receiver._record_trade("BTCUSDT", 20_400, 60_000)

        self.assertTrue(accepted)
        self.observer.observe.assert_called_once_with(
            "BTCUSDT", 20_400, 60_000
        )

    def test_out_of_order_trade_is_not_forwarded(self) -> None:
        self.store.update.return_value = False

        accepted = self.receiver._record_trade("BTCUSDT", 20_000, 59_999)

        self.assertFalse(accepted)
        self.observer.observe.assert_not_called()

    def test_observer_failure_is_fatal_instead_of_silently_reconnecting(self) -> None:
        self.store.update.return_value = True
        self.observer.observe.side_effect = ValueError("boom")

        with self.assertRaises(TradeProcessingError):
            self.receiver._record_trade("BTCUSDT", 20_400, 60_000)


if __name__ == "__main__":
    unittest.main()
