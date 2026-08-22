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
        self.assertEqual(config.window_seconds, 120)
        self.assertEqual(config.threshold_pct, 3)
        self.assertEqual(config.cooldown_seconds, 30)

    def test_deployment_environment_wins_over_dotenv(self) -> None:
        config = self.load(THRESHOLD_PCT="6")

        self.assertEqual(config.threshold_pct, 6)

    def test_rejects_invalid_webhook_url(self) -> None:
        with self.assertRaisesRegex(ConfigError, "CALL_WEBHOOK_URL"):
            self.load(CALL_WEBHOOK_URL="not-a-url")

    def test_rejects_excessive_webhook_retries(self) -> None:
        with self.assertRaisesRegex(ConfigError, "no greater than 10"):
            self.load(WEBHOOK_MAX_RETRIES="11")

    def test_legacy_webhook_variable_remains_supported(self) -> None:
        with patch.dict(
            os.environ, {"WEBHOOK_URL": "https://example.com/legacy"}, clear=True
        ):
            config = load_config(env_path=MISSING_ENV_FILE)

        self.assertEqual(config.webhook.url, "https://example.com/legacy")


if __name__ == "__main__":
    unittest.main()
