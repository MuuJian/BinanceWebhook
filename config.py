"""Validated environment-based worker configuration."""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

DEFAULT_SYMBOLS = "BTCUSDT,ETHUSDT,SOLUSDT"
DEFAULT_CHANGE_LEVELS = "3,6,9"


class ConfigError(ValueError):
    """Raised when required environment configuration is invalid."""


@dataclass(frozen=True)
class Config:
    webhook_url: str
    webhook_body_format: str
    alert_symbols: tuple[str, ...]
    alert_window_seconds: int
    alert_change_levels: tuple[float, ...]
    webhook_timeout_seconds: float


def _positive_int(name: str, default: str) -> int:
    raw = os.getenv(name, default).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ConfigError(f"{name} must be greater than zero")
    return value


def _positive_float(name: str, default: str) -> float:
    raw = os.getenv(name, default).strip()
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number") from exc
    if not math.isfinite(value) or value <= 0:
        raise ConfigError(f"{name} must be a finite number greater than zero")
    return value


def _parse_symbols() -> tuple[str, ...]:
    parts = [item.strip().upper() for item in os.getenv(
        "ALERT_SYMBOLS", DEFAULT_SYMBOLS
    ).split(",")]
    if not parts or any(not symbol for symbol in parts):
        raise ConfigError("ALERT_SYMBOLS must be a comma-separated symbol list")
    if any(not symbol.isalnum() for symbol in parts):
        raise ConfigError("ALERT_SYMBOLS may contain only letters and numbers")
    return tuple(dict.fromkeys(parts))


def _parse_change_levels() -> tuple[float, ...]:
    parts = [item.strip() for item in os.getenv(
        "ALERT_CHANGE_LEVELS", DEFAULT_CHANGE_LEVELS
    ).split(",")]
    if not parts or any(not item for item in parts):
        raise ConfigError(
            "ALERT_CHANGE_LEVELS must be a comma-separated number list"
        )
    try:
        levels = tuple(float(item) for item in parts)
    except ValueError as exc:
        raise ConfigError("ALERT_CHANGE_LEVELS must contain only numbers") from exc
    if any(not math.isfinite(level) or level <= 0 for level in levels):
        raise ConfigError(
            "ALERT_CHANGE_LEVELS must contain finite positive numbers"
        )
    if any(current <= previous for previous, current in zip(levels, levels[1:])):
        raise ConfigError("ALERT_CHANGE_LEVELS must be strictly increasing")
    return levels


def load_config(*, read_dotenv: bool = True) -> Config:
    if read_dotenv:
        load_dotenv(Path(__file__).with_name(".env"))

    webhook_url = os.getenv("WEBHOOK_URL", "").strip()
    parsed_url = urlparse(webhook_url)
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
        raise ConfigError("WEBHOOK_URL must be a valid http:// or https:// URL")

    body_format = os.getenv("WEBHOOK_BODY_FORMAT", "json").strip().lower()
    if body_format not in {"json", "text"}:
        raise ConfigError("WEBHOOK_BODY_FORMAT must be either json or text")

    return Config(
        webhook_url=webhook_url,
        webhook_body_format=body_format,
        alert_symbols=_parse_symbols(),
        alert_window_seconds=_positive_int("ALERT_WINDOW_SECONDS", "300"),
        alert_change_levels=_parse_change_levels(),
        webhook_timeout_seconds=_positive_float("WEBHOOK_TIMEOUT_SECONDS", "10"),
    )
