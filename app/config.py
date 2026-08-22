"""Environment-only configuration for the Binance alert worker."""

from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"
FIXED_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
BINANCE_COMBINED_STREAM_URL = "wss://fstream.binance.com/market/stream?streams="
MAX_WEBHOOK_RETRIES = 10


class ConfigError(ValueError):
    """Raised when an environment setting is missing or invalid."""


@dataclass(frozen=True, slots=True)
class WebhookConfig:
    url: str
    timeout_seconds: float
    max_retries: int


@dataclass(frozen=True, slots=True)
class AppConfig:
    symbols: tuple[str, ...]
    websocket_url: str
    websocket_proxy: str | None
    window_seconds: int
    threshold_pct: float
    cooldown_seconds: float
    evaluation_interval_seconds: float
    webhook: WebhookConfig
    log_level: int


def _websocket_url(symbols: tuple[str, ...]) -> str:
    streams = "/".join(f"{symbol.lower()}@aggTrade" for symbol in symbols)
    return f"{BINANCE_COMBINED_STREAM_URL}{streams}"


def _positive_float(name: str, default: str) -> float:
    raw = os.getenv(name, default).strip()
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number greater than zero") from exc
    if not math.isfinite(value) or value <= 0:
        raise ConfigError(f"{name} must be a finite number greater than zero")
    return value


def _positive_int(name: str, default: str, *, minimum: int = 1) -> int:
    raw = os.getenv(name, default).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc
    if str(value) != raw or value < minimum:
        raise ConfigError(f"{name} must be an integer of at least {minimum}")
    return value


def _nonnegative_int(
    name: str, default: str, *, maximum: int | None = None
) -> int:
    raw = os.getenv(name, default).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a non-negative integer") from exc
    if str(value) != raw or value < 0:
        raise ConfigError(f"{name} must be a non-negative integer")
    if maximum is not None and value > maximum:
        raise ConfigError(f"{name} must be no greater than {maximum}")
    return value


def _webhook_url() -> str:
    # WEBHOOK_URL keeps existing local installations working during migration.
    value = os.getenv("CALL_WEBHOOK_URL", "").strip()
    if not value:
        value = os.getenv("WEBHOOK_URL", "").strip()
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ConfigError(
            "CALL_WEBHOOK_URL must be a valid http:// or https:// URL"
        )
    return value


def _proxy_url() -> str | None:
    value = os.getenv("WS_PROXY_URL", "").strip()
    if not value:
        return None
    parsed = urlparse(value)
    supported = {"http", "https", "socks4", "socks4a", "socks5", "socks5h"}
    if parsed.scheme.lower() not in supported or not parsed.hostname:
        raise ConfigError("WS_PROXY_URL has an unsupported or invalid proxy URL")
    return value


def _log_level() -> int:
    name = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    if name not in levels:
        raise ConfigError(
            "LOG_LEVEL must be DEBUG, INFO, WARNING, ERROR, or CRITICAL"
        )
    return levels[name]


def load_config(*, env_path: Path = DEFAULT_ENV_PATH) -> AppConfig:
    """Load local .env values without overriding deployment variables."""

    load_dotenv(env_path, override=False)
    window_seconds = _positive_int("WINDOW_SECONDS", "120")
    threshold_pct = _positive_float("THRESHOLD_PCT", "3")

    return AppConfig(
        symbols=FIXED_SYMBOLS,
        websocket_url=_websocket_url(FIXED_SYMBOLS),
        websocket_proxy=_proxy_url(),
        window_seconds=window_seconds,
        threshold_pct=threshold_pct,
        cooldown_seconds=_positive_float("COOLDOWN_SECONDS", "30"),
        evaluation_interval_seconds=_positive_float(
            "EVALUATION_INTERVAL_SECONDS", "1"
        ),
        webhook=WebhookConfig(
            url=_webhook_url(),
            timeout_seconds=_positive_float("WEBHOOK_TIMEOUT_SECONDS", "10"),
            max_retries=_nonnegative_int(
                "WEBHOOK_MAX_RETRIES", "3", maximum=MAX_WEBHOOK_RETRIES
            ),
        ),
        log_level=_log_level(),
    )
