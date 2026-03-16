# multitool/utils/settings.py
"""Application settings persistence and rate limit parameter derivation."""

import configparser
import os

from ..constants import (
    CONFIG_DIR,
    CONFIG_FILE,
    DEFAULT_CH_RATE_LIMIT,
    DEFAULT_CH_BURST_CAPACITY,
    DEFAULT_CH_MAX_WORKERS,
    MAX_CH_MAX_WORKERS,
    MIN_CH_MAX_WORKERS,
)

DEFAULTS = {
    "appearance": {
        "dark_theme": "true",
        "font_size": "10",
    },
    "rate_limiting": {
        "ch_rate_limit": str(DEFAULT_CH_RATE_LIMIT),
        "ch_max_workers": str(DEFAULT_CH_MAX_WORKERS),
        "ch_burst_capacity": str(DEFAULT_CH_BURST_CAPACITY),
    },
}


def derive_rate_params(rate_limit: int) -> dict:
    """
    Derive token bucket and concurrency parameters from a rate limit.

    Given a rate limit in requests per 5 minutes, derives:
    - refill_rate: tokens per second (with 90% safety margin)
    - capacity: burst capacity (15-second burst window)
    - max_workers: concurrent thread count

    Args:
        rate_limit: Maximum requests allowed per 5 minutes.

    Returns:
        Dict with keys 'refill_rate', 'capacity', 'max_workers'.
    """
    safety_margin = 0.90
    effective_rate = rate_limit * safety_margin
    refill_rate = effective_rate / 300  # 5 minutes = 300 seconds
    capacity = max(10, int(refill_rate * 15))  # 15-second burst window
    max_workers = min(MAX_CH_MAX_WORKERS, max(MIN_CH_MAX_WORKERS, rate_limit // 200))
    return {
        "refill_rate": round(refill_rate, 4),
        "capacity": capacity,
        "max_workers": max_workers,
    }


def load_settings() -> dict:
    """
    Load settings from config.ini, returning defaults for any missing values.

    Returns:
        Dict with keys: dark_theme (bool), font_size (int),
        ch_rate_limit (int), ch_max_workers (int), ch_burst_capacity (int).
    """
    config = configparser.ConfigParser()

    if os.path.exists(CONFIG_FILE):
        config.read(CONFIG_FILE)

    return {
        "dark_theme": config.getboolean(
            "Appearance", "dark_theme",
            fallback=DEFAULTS["appearance"]["dark_theme"] == "true"
        ),
        "font_size": config.getint(
            "Appearance", "font_size",
            fallback=int(DEFAULTS["appearance"]["font_size"])
        ),
        "ch_rate_limit": config.getint(
            "RateLimiting", "ch_rate_limit",
            fallback=int(DEFAULTS["rate_limiting"]["ch_rate_limit"])
        ),
        "ch_max_workers": config.getint(
            "RateLimiting", "ch_max_workers",
            fallback=int(DEFAULTS["rate_limiting"]["ch_max_workers"])
        ),
        "ch_burst_capacity": config.getint(
            "RateLimiting", "ch_burst_capacity",
            fallback=int(DEFAULTS["rate_limiting"]["ch_burst_capacity"])
        ),
    }


def save_settings(settings: dict) -> None:
    """
    Save settings to config.ini.

    Args:
        settings: Dict with the same keys as returned by load_settings().
    """
    os.makedirs(CONFIG_DIR, exist_ok=True)

    config = configparser.ConfigParser()

    config["Appearance"] = {
        "dark_theme": str(settings.get("dark_theme", True)).lower(),
        "font_size": str(settings.get("font_size", 10)),
    }

    config["RateLimiting"] = {
        "ch_rate_limit": str(settings.get("ch_rate_limit", DEFAULT_CH_RATE_LIMIT)),
        "ch_max_workers": str(settings.get("ch_max_workers", DEFAULT_CH_MAX_WORKERS)),
        "ch_burst_capacity": str(settings.get("ch_burst_capacity", DEFAULT_CH_BURST_CAPACITY)),
    }

    with open(CONFIG_FILE, "w") as f:
        config.write(f)
