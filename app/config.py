"""Load and validate YAML business settings and private environment values."""

from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.yaml"
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"
FIXED_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
FIXED_WEBSOCKET_URL = (
    "wss://fstream.binance.com/market/stream?streams="
    "btcusdt@miniTicker/ethusdt@miniTicker/solusdt@miniTicker"
)


class ConfigError(ValueError):
    """Raised when a required setting is missing or invalid."""


@dataclass(frozen=True, slots=True)
class WebhookConfig:
    url: str
    timeout_seconds: float
    max_retries: int


@dataclass(frozen=True, slots=True)
class AppConfig:
    symbols: tuple[str, ...]
    websocket_url: str
    window_seconds: int
    threshold_pct: float
    cooldown_seconds: float
    evaluation_interval_seconds: float
    min_points: int
    warmup_seconds: float
    webhook: WebhookConfig
    log_level: int


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigError(f"{name} must be a YAML mapping")
    return value


def _positive_number(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ConfigError(f"{name} must be a positive number")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{name} must be a positive number") from exc
    if not math.isfinite(number) or number <= 0:
        raise ConfigError(f"{name} must be a finite number greater than zero")
    return number


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise ConfigError(f"{name} must be a positive integer")
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{name} must be a positive integer") from exc
    if isinstance(value, float) and not value.is_integer():
        raise ConfigError(f"{name} must be a positive integer")
    if isinstance(value, str) and str(number) != value.strip():
        raise ConfigError(f"{name} must be a positive integer")
    if number <= 0:
        raise ConfigError(f"{name} must be a positive integer")
    return number


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as config_file:
            raw = yaml.safe_load(config_file)
    except FileNotFoundError as exc:
        raise ConfigError(f"Configuration file not found: {path}") from exc
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigError(
            f"Unable to read configuration file: {type(exc).__name__}"
        ) from exc
    return _mapping(raw, "config.yaml")


def _validate_webhook_url(value: str) -> str:
    url = value.strip()
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ConfigError("CALL_WEBHOOK_URL must be a valid http:// or https:// URL")
    return url


def _parse_log_level(value: Any) -> int:
    if not isinstance(value, str):
        raise ConfigError("LOG_LEVEL must be a string")
    level_name = value.strip().upper()
    levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    try:
        return levels[level_name]
    except KeyError as exc:
        raise ConfigError(
            "LOG_LEVEL must be DEBUG, INFO, WARNING, ERROR, or CRITICAL"
        ) from exc


def _setting(raw: dict[str, Any], key: str, environment_name: str) -> Any:
    """Use a dashboard environment variable when present, else YAML."""

    if environment_name in os.environ:
        return os.environ[environment_name].strip()
    return raw.get(key)


def load_config(
    *,
    config_path: Path = DEFAULT_CONFIG_PATH,
    env_path: Path = DEFAULT_ENV_PATH,
) -> AppConfig:
    """Load configuration without allowing .env to override deployed variables."""

    load_dotenv(env_path, override=False)
    raw = _load_yaml(config_path)

    webhook_raw = _mapping(raw.get("webhook"), "webhook")

    window_seconds = _positive_int(
        _setting(raw, "window_seconds", "WINDOW_SECONDS"), "window_seconds"
    )
    threshold_pct = _positive_number(
        _setting(raw, "threshold_pct", "THRESHOLD_PCT"), "threshold_pct"
    )
    cooldown_seconds = _positive_number(
        _setting(raw, "cooldown_seconds", "COOLDOWN_SECONDS"),
        "cooldown_seconds",
    )
    evaluation_interval_seconds = _positive_number(
        _setting(
            raw,
            "evaluation_interval_seconds",
            "EVALUATION_INTERVAL_SECONDS",
        ),
        "evaluation_interval_seconds",
    )
    min_points = _positive_int(
        _setting(raw, "min_points", "MIN_POINTS"), "min_points"
    )
    warmup_seconds = _positive_number(
        _setting(raw, "warmup_seconds", "WARMUP_SECONDS"), "warmup_seconds"
    )
    if min_points < 2:
        raise ConfigError("min_points must be at least 2")
    if warmup_seconds > window_seconds:
        raise ConfigError("warmup_seconds must not exceed window_seconds")

    # WEBHOOK_URL remains a private, temporary compatibility fallback so an
    # existing local .env keeps working while deployments migrate to the name
    # required by the new specification.
    webhook_url = os.getenv("CALL_WEBHOOK_URL", "").strip()
    if not webhook_url:
        webhook_url = os.getenv("WEBHOOK_URL", "").strip()

    return AppConfig(
        symbols=FIXED_SYMBOLS,
        websocket_url=FIXED_WEBSOCKET_URL,
        window_seconds=window_seconds,
        threshold_pct=threshold_pct,
        cooldown_seconds=cooldown_seconds,
        evaluation_interval_seconds=evaluation_interval_seconds,
        min_points=min_points,
        warmup_seconds=warmup_seconds,
        webhook=WebhookConfig(
            url=_validate_webhook_url(webhook_url),
            timeout_seconds=_positive_number(
                _setting(
                    webhook_raw,
                    "timeout_seconds",
                    "WEBHOOK_TIMEOUT_SECONDS",
                ),
                "webhook.timeout_seconds",
            ),
            max_retries=_positive_int(
                _setting(webhook_raw, "max_retries", "WEBHOOK_MAX_RETRIES"),
                "webhook.max_retries",
            ),
        ),
        log_level=_parse_log_level(os.getenv("LOG_LEVEL", "INFO")),
    )
