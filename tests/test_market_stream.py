from __future__ import annotations

import json
import unittest
from unittest.mock import Mock

from app.market_stream import BinanceAggTradeReceiver, _reconnect_delay


class MarketMessageParsingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.receiver = BinanceAggTradeReceiver(
            websocket_url="wss://example.com",
            websocket_proxy=None,
            symbols=("BTCUSDT",),
            store=Mock(),
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

    def test_reconnect_backoff_is_bounded_for_huge_failure_counts(self) -> None:
        self.assertEqual(
            [_reconnect_delay(failures) for failures in range(7)],
            [1, 2, 4, 8, 16, 30, 30],
        )
        self.assertEqual(_reconnect_delay(1_000_000), 30)


if __name__ == "__main__":
    unittest.main()
