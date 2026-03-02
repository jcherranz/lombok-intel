"""Shared utilities for zone assignment, logging, and rate limiting."""

import logging
import random
import time
from datetime import datetime

from src.config import ZONE_BOUNDS, SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Create a configured logger."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter(
            "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(fmt)
        logger.addHandler(handler)
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
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
