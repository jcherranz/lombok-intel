"""Shared configuration for Lombok Market Intelligence."""

from pathlib import Path

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "lombok_intel.db"
GEOJSON_PATH = DATA_DIR / "lombok_zones.geojson"

# Lombok bounding box (used for Airbnb search queries)
LOMBOK_BOUNDS = {
    "ne_lat": -8.15,
    "ne_lng": 116.80,
    "sw_lat": -9.00,
    "sw_lng": 115.85,
}

# Zone definitions with center points for search queries
ZONES = {
    "GLI": {"name": "Gili Islands", "lat": -8.35, "lng": 116.06, "radius_km": 8},
    "SGG": {"name": "Senggigi", "lat": -8.49, "lng": 116.05, "radius_km": 10},
    "NLB": {"name": "North Lombok / Sire", "lat": -8.30, "lng": 116.25, "radius_km": 15},
    "MTR": {"name": "Mataram", "lat": -8.58, "lng": 116.12, "radius_km": 10},
    "KUT": {"name": "Kuta / Mandalika", "lat": -8.90, "lng": 116.30, "radius_km": 8},
    "TAA": {"name": "Tanjung Aan / Gerupuk", "lat": -8.91, "lng": 116.43, "radius_km": 10},
    "SBK": {"name": "Selong Belanak / West Surf", "lat": -8.90, "lng": 116.16, "radius_km": 12},
    "SKT": {"name": "Sekotong", "lat": -8.74, "lng": 115.98, "radius_km": 15},
}

# Zone bounding boxes for point-in-polygon assignment (matches schema.sql)
ZONE_BOUNDS = {
    "GLI": {"lat_min": -8.38, "lat_max": -8.30, "lng_min": 116.00, "lng_max": 116.10, "priority": 10},
    "SGG": {"lat_min": -8.55, "lat_max": -8.38, "lng_min": 116.01, "lng_max": 116.10, "priority": 20},
    "NLB": {"lat_min": -8.38, "lat_max": -8.15, "lng_min": 116.10, "lng_max": 116.45, "priority": 30},
    "MTR": {"lat_min": -8.65, "lat_max": -8.50, "lng_min": 116.05, "lng_max": 116.20, "priority": 40},
    "KUT": {"lat_min": -8.92, "lat_max": -8.80, "lng_min": 116.23, "lng_max": 116.38, "priority": 10},
    "TAA": {"lat_min": -8.92, "lat_max": -8.82, "lng_min": 116.35, "lng_max": 116.52, "priority": 20},
    "SBK": {"lat_min": -8.95, "lat_max": -8.82, "lng_min": 116.05, "lng_max": 116.27, "priority": 20},
    "SKT": {"lat_min": -8.95, "lat_max": -8.60, "lng_min": 115.88, "lng_max": 116.08, "priority": 30},
}

# Scraping settings
SCRAPE_DELAY_MIN = 2.0  # seconds between requests
SCRAPE_DELAY_MAX = 5.0  # seconds between requests
CALENDAR_DAYS_FORWARD = 90  # how many days of calendar to poll
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30  # seconds

# Airbnb search settings
AIRBNB_CURRENCY = "USD"
AIRBNB_RESULTS_PER_PAGE = 50

# Booking.com settings
BOOKING_CURRENCY = "USD"
BOOKING_LANGUAGE = "en-us"
