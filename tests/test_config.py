from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from app.config import ConfigError, FIXED_SYMBOLS, load_config


MISSING_ENV_FILE = Path("/tmp/binance-webhook-tests-no-env-file")


class ConfigTests(unittest.TestCase):
    def load(self, **values: str):
        environment = {"CALL_WEBHOOK_URL": "https://example.com/hook", **values}
        with patch.dict(os.environ, environment, clear=True):
            return load_config(env_path=MISSING_ENV_FILE)

    def test_defaults_and_stream_url_share_one_symbol_source(self) -> None:
        config = self.load()

        self.assertEqual(config.symbols, FIXED_SYMBOLS)
        self.assertEqual(
            config.websocket_url,
            "wss://fstream.binance.com/market/stream?streams="
            "btcusdt@aggTrade/ethusdt@aggTrade/solusdt@aggTrade",
        )
        self.assertEqual(config.reset_pct, 2)

    def test_deployment_environment_wins_over_dotenv(self) -> None:
        config = self.load(THRESHOLD_PCT="6")

        self.assertEqual(config.threshold_pct, 6)
        self.assertEqual(config.reset_pct, 4)

    def test_rejects_warmup_longer_than_window(self) -> None:
        with self.assertRaisesRegex(
            ConfigError, "WARMUP_SECONDS must not exceed WINDOW_SECONDS"
        ):
            self.load(WINDOW_SECONDS="30", WARMUP_SECONDS="31")

    def test_rejects_reset_at_or_above_threshold(self) -> None:
        with self.assertRaisesRegex(
            ConfigError, "RESET_PCT must be less than THRESHOLD_PCT"
        ):
            self.load(THRESHOLD_PCT="3", RESET_PCT="3")

    def test_rejects_invalid_webhook_url(self) -> None:
        with self.assertRaisesRegex(ConfigError, "CALL_WEBHOOK_URL"):
            self.load(CALL_WEBHOOK_URL="not-a-url")

    def test_legacy_webhook_variable_remains_supported(self) -> None:
        with patch.dict(
            os.environ, {"WEBHOOK_URL": "https://example.com/legacy"}, clear=True
        ):
            config = load_config(env_path=MISSING_ENV_FILE)

        self.assertEqual(config.webhook.url, "https://example.com/legacy")


if __name__ == "__main__":
    unittest.main()
