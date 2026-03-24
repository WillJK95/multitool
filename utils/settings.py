# multitool/utils/settings.py
"""Application settings persistence and rate limit parameter derivation."""

import configparser
import os

from ..constants import (
    CONFIG_DIR,
    CONFIG_FILE,
    RECENT_REPORTS_FILE,
    DEFAULT_CH_PACING_MODE,
    DEFAULT_CH_MAX_WORKERS,
    INITIAL_RATE_LIMIT,
    MAX_CH_MAX_WORKERS,
    MIN_CH_MAX_WORKERS,
    SMOOTH_BURST_WINDOW_SECONDS,
    SMOOTH_SAFETY_MARGIN,
)

DEFAULTS = {
    "appearance": {
        "dark_theme": "true",
        "font_size": "10",
    },
    "rate_limiting": {
        "ch_pacing_mode": DEFAULT_CH_PACING_MODE,
        "ch_max_workers": str(DEFAULT_CH_MAX_WORKERS),
    },
}


def derive_initial_params(pacing_mode: str) -> dict:
    """
    Derive token bucket parameters for app startup (before first API response).

    Uses a conservative initial rate limit. Once the first API response
    arrives, sync_from_headers() takes over and these values are replaced.

    Args:
        pacing_mode: "smooth" or "burst".

    Returns:
        Dict with keys 'refill_rate' and 'capacity'.
    """
    rate_limit = INITIAL_RATE_LIMIT
    raw_rate = rate_limit / 300  # 5 minutes = 300 seconds

    if pacing_mode == "burst":
        refill_rate = raw_rate
        capacity = rate_limit
    else:
        refill_rate = raw_rate * SMOOTH_SAFETY_MARGIN
        capacity = max(10, int(refill_rate * SMOOTH_BURST_WINDOW_SECONDS))

    return {
        "refill_rate": round(refill_rate, 4),
        "capacity": capacity,
    }


def load_settings() -> dict:
    """
    Load settings from config.ini, returning defaults for any missing values.

    Returns:
        Dict with keys: dark_theme (bool), font_size (int),
        ch_pacing_mode (str), ch_max_workers (int).
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
        "ch_pacing_mode": config.get(
            "RateLimiting", "ch_pacing_mode",
            fallback=DEFAULTS["rate_limiting"]["ch_pacing_mode"]
        ),
        "ch_max_workers": config.getint(
            "RateLimiting", "ch_max_workers",
            fallback=int(DEFAULTS["rate_limiting"]["ch_max_workers"])
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
        "ch_pacing_mode": settings.get("ch_pacing_mode", DEFAULT_CH_PACING_MODE),
        "ch_max_workers": str(settings.get("ch_max_workers", DEFAULT_CH_MAX_WORKERS)),
    }

    with open(CONFIG_FILE, "w") as f:
        config.write(f)


def save_recent_reports(reports: list) -> None:
    """Persist recent EDD reports list to JSON. Filters out missing files."""
    import json
    os.makedirs(CONFIG_DIR, exist_ok=True)
    valid = [r for r in reports if os.path.exists(r.get("path", ""))]
    with open(RECENT_REPORTS_FILE, "w", encoding="utf-8") as f:
        json.dump(valid[:5], f, indent=2)


def load_recent_reports() -> list:
    """Load recent EDD reports from JSON, filtering out missing files."""
    import json
    if not os.path.exists(RECENT_REPORTS_FILE):
        return []
    try:
        with open(RECENT_REPORTS_FILE, "r", encoding="utf-8") as f:
            reports = json.load(f)
        return [r for r in reports if os.path.exists(r.get("path", ""))]
    except Exception:
        return []
