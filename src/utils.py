"""Shared utilities for zone assignment, logging, and rate limiting."""

import logging
import random
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

from src.config import ZONE_BOUNDS, SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX

LOG_DIR = Path(__file__).resolve().parent.parent / "data" / "logs"


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Create a configured logger with console (WARNING+) and file (DEBUG+) output."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        fmt = logging.Formatter(
            "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Console: WARNING and above
        console = logging.StreamHandler()
        console.setLevel(logging.WARNING)
        console.setFormatter(fmt)
        logger.addHandler(console)

        # File: DEBUG and above, with rotation
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            LOG_DIR / "lombok_intel.log",
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=5,
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)

    logger.setLevel(level)
    return logger


def assign_zone(lat: float | None, lng: float | None) -> str | None:
    """Assign a listing to a zone based on lat/lng using bounding boxes.

    When a point falls in overlapping zones, the zone with the lowest
    priority number wins.
    """
    if lat is None or lng is None:
        return None

    matches = []
    for zone_id, bounds in ZONE_BOUNDS.items():
        if (bounds["lat_min"] <= lat <= bounds["lat_max"]
                and bounds["lng_min"] <= lng <= bounds["lng_max"]):
            matches.append((bounds["priority"], zone_id))

    if not matches:
        return None

    matches.sort()
    return matches[0][1]


def rate_limit():
    """Sleep a random interval to avoid detection."""
    delay = random.uniform(SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX)
    time.sleep(delay)


def now_iso() -> str:
    """Return current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Data validation
# ---------------------------------------------------------------------------

# Lombok approximate bounds (generous margin around the island)
_LOMBOK_LAT_MIN, _LOMBOK_LAT_MAX = -9.1, -8.1
_LOMBOK_LNG_MIN, _LOMBOK_LNG_MAX = 115.8, 116.9

_validation_logger = logging.getLogger("validation")


def validate_coordinates(lat: float | None, lng: float | None) -> bool:
    """Check if coordinates fall within Lombok bounds. Warns but returns True/False."""
    if lat is None or lng is None:
        return True  # missing coords are not invalid, just unknown
    ok = (_LOMBOK_LAT_MIN <= lat <= _LOMBOK_LAT_MAX
          and _LOMBOK_LNG_MIN <= lng <= _LOMBOK_LNG_MAX)
    if not ok:
        _validation_logger.warning(
            "Coordinates outside Lombok bounds: lat=%.6f, lng=%.6f", lat, lng
        )
    return ok


def validate_price(price: float | None) -> bool:
    """Check if price is within sanity range ($5-$5000/night). Warns but returns True/False."""
    if price is None:
        return True  # missing price is not invalid
    ok = 5.0 <= price <= 5000.0
    if not ok:
        _validation_logger.warning("Price outside sanity range: $%.2f", price)
    return ok


# ---------------------------------------------------------------------------
# Telegram notifications
# ---------------------------------------------------------------------------

def notify_telegram(message: str) -> bool:
    """Send a message via Telegram bot. Returns True on success.

    Requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables.
    Silently returns False if not configured.
    """
    import os
    import urllib.request
    import urllib.parse

    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return False

    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown",
        }).encode()
        req = urllib.request.Request(url, data=data)
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception as e:
        logging.getLogger("telegram").warning("Telegram notification failed: %s", e)
        return False
