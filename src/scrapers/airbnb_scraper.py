"""Airbnb scraper for Lombok Market Intelligence.

Discovers listings across 8 Lombok investment zones and scrapes calendar
availability up to 90 days forward using the pyairbnb library (GraphQL v3 API).
Persists results to SQLite via the project schema.
"""

import sqlite3
from datetime import date, timedelta
from typing import Any

import pyairbnb
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from src.config import (
    AIRBNB_CURRENCY,
    CALENDAR_DAYS_FORWARD,
    DB_PATH,
    MAX_RETRIES,
    ZONE_BOUNDS,
    ZONES,
)
from src.db.init_db import get_connection
from src.utils import assign_zone, now_iso, rate_limit, setup_logger, validate_coordinates, validate_price

logger = setup_logger("airbnb_scraper")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(val: Any) -> float | None:
    """Coerce a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_int(val: Any) -> int | None:
    """Coerce a value to int, returning None on failure."""
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _bool_to_int(val: Any) -> int:
    """Convert a truthy value to 0/1 for SQLite boolean columns."""
    return 1 if val else 0


def _check_in_out() -> tuple[str, str]:
    """Return a (check_in, check_out) pair for tomorrow/day-after.

    pyairbnb search requires check-in/check-out dates. We use tomorrow
    so the API returns current pricing and availability.
    """
    tomorrow = date.today() + timedelta(days=1)
    day_after = tomorrow + timedelta(days=1)
    return tomorrow.isoformat(), day_after.isoformat()


def _subdivide_box(
    ne_lat: float, ne_long: float, sw_lat: float, sw_long: float
) -> list[dict[str, float]]:
    """Split a bounding box into 4 equal quadrants.

    Used when a zone search returns the Airbnb cap (~300 results),
    suggesting there are more listings that were not returned.
    """
    mid_lat = (ne_lat + sw_lat) / 2
    mid_long = (ne_long + sw_long) / 2
    return [
        {"ne_lat": ne_lat, "ne_long": ne_long, "sw_lat": mid_lat, "sw_long": mid_long},  # NE quadrant
        {"ne_lat": ne_lat, "ne_long": mid_long, "sw_lat": mid_lat, "sw_long": sw_long},  # NW quadrant
        {"ne_lat": mid_lat, "ne_long": ne_long, "sw_lat": sw_lat, "sw_long": mid_long},  # SE quadrant
        {"ne_lat": mid_lat, "ne_long": mid_long, "sw_lat": sw_lat, "sw_long": sw_long},  # SW quadrant
    ]


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class AirbnbScraper:
    """Discovers Airbnb listings in Lombok zones and scrapes their calendars."""

    # Airbnb search results are capped around 300; if we hit this threshold
    # we subdivide the search box to avoid missing listings.
    _RESULT_CAP = 300

    def __init__(self, db_path=None, proxy_url: str | None = None):
        self.db_path = db_path or DB_PATH
        self.proxy_url = proxy_url or ""
        self.conn: sqlite3.Connection | None = None
        self._api_key: str | None = None

        # Counters for the current run
        self._listings_seen = 0
        self._listings_new = 0
        self._snapshots_added = 0
        self._errors: list[str] = []

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type(Exception),
        before_sleep=before_sleep_log(logger, 20),
        reraise=True,
    )
    def _fetch_api_key(self) -> str:
        """Fetch the Airbnb API key with retry logic."""
        return pyairbnb.get_api_key(proxy_url=self.proxy_url)

    def _get_api_key(self) -> str:
        """Fetch and cache the Airbnb API key needed for calendar/details calls."""
        if self._api_key is None:
            logger.info("Fetching Airbnb API key...")
            self._api_key = self._fetch_api_key()
            logger.info("API key obtained.")
        return self._api_key

    # ------------------------------------------------------------------
    # Database helpers
    # ------------------------------------------------------------------

    def _get_conn(self) -> sqlite3.Connection:
        if self.conn is None:
            self.conn = get_connection(self.db_path)
        return self.conn

    def _start_run(self, run_type: str = "full") -> int:
        """Insert a new scrape_runs row and return the run_id."""
        conn = self._get_conn()
        cursor = conn.execute(
            """
            INSERT INTO scrape_runs (source, run_type, started_at, status)
            VALUES ('airbnb', ?, ?, 'running')
            """,
            (run_type, now_iso()),
        )
        conn.commit()
        run_id = cursor.lastrowid
        logger.info("Started scrape run %d (type=%s)", run_id, run_type)
        return run_id

    def _finish_run(self, run_id: int, status: str = "completed", error_message: str | None = None):
        """Update the scrape_runs row when the run ends."""
        conn = self._get_conn()
        conn.execute(
            """
            UPDATE scrape_runs
            SET finished_at = ?,
                status = ?,
                listings_seen = ?,
                listings_new = ?,
                snapshots_added = ?,
                error_message = ?
            WHERE run_id = ?
            """,
            (
                now_iso(),
                status,
                self._listings_seen,
                self._listings_new,
                self._snapshots_added,
                error_message,
                run_id,
            ),
        )
        conn.commit()
        logger.info(
            "Run %d finished: status=%s, seen=%d, new=%d, snapshots=%d",
            run_id, status, self._listings_seen, self._listings_new, self._snapshots_added,
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=16),
        retry=retry_if_exception_type(sqlite3.OperationalError),
        before_sleep=before_sleep_log(logger, 20),
        reraise=True,
    )
    def _upsert_listing(self, data: dict[str, Any], run_id: int) -> bool:
        """Upsert a single listing into airbnb_listings.

        Returns True if the listing was newly inserted, False if updated.
        Handles both search_all() result format and get_details() format.
        """
        conn = self._get_conn()
        listing_id = str(data.get("room_id") or data.get("id") or "")
        if not listing_id:
            return False

        coords = data.get("coordinates", {})
        lat = _safe_float(coords.get("latitude") or data.get("lat") or data.get("latitude"))
        # Note: pyairbnb has a typo — "longitud" instead of "longitude"
        lng = _safe_float(coords.get("longitud") or coords.get("longitude") or data.get("lng") or data.get("longitude"))
        validate_coordinates(lat, lng)
        zone_id = assign_zone(lat, lng)

        # Validate price
        nightly_price = _safe_float(
            data.get("nightly_price")
            or (data.get("price", {}).get("unit", {}).get("amount") if isinstance(data.get("price"), dict) else data.get("price"))
        )
        validate_price(nightly_price)

        now = now_iso()

        # Check if listing already exists
        existing = conn.execute(
            "SELECT listing_id FROM airbnb_listings WHERE listing_id = ?",
            (listing_id,),
        ).fetchone()
        is_new = existing is None

        conn.execute(
            """
            INSERT INTO airbnb_listings (
                listing_id, url, name, description,
                property_type, room_type,
                latitude, longitude, zone_id, neighborhood,
                accommodates, bedrooms, beds, bathrooms,
                nightly_price, cleaning_fee, currency,
                host_id, host_name, is_superhost,
                rating_overall, rating_accuracy, rating_checkin,
                rating_cleanliness, rating_communication,
                rating_location, rating_value,
                review_count, reviews_per_month,
                instant_bookable, minimum_nights, maximum_nights,
                first_scraped_at, last_scraped_at, is_active, last_run_id
            ) VALUES (
                ?, ?, ?, ?,
                ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?, 1, ?
            )
            ON CONFLICT(listing_id) DO UPDATE SET
                url = excluded.url,
                name = excluded.name,
                description = excluded.description,
                property_type = excluded.property_type,
                room_type = excluded.room_type,
                latitude = excluded.latitude,
                longitude = excluded.longitude,
                zone_id = excluded.zone_id,
                neighborhood = excluded.neighborhood,
                accommodates = excluded.accommodates,
                bedrooms = excluded.bedrooms,
                beds = excluded.beds,
                bathrooms = excluded.bathrooms,
                nightly_price = excluded.nightly_price,
                cleaning_fee = excluded.cleaning_fee,
                currency = excluded.currency,
                host_id = excluded.host_id,
                host_name = excluded.host_name,
                is_superhost = excluded.is_superhost,
                rating_overall = excluded.rating_overall,
                rating_accuracy = excluded.rating_accuracy,
                rating_checkin = excluded.rating_checkin,
                rating_cleanliness = excluded.rating_cleanliness,
                rating_communication = excluded.rating_communication,
                rating_location = excluded.rating_location,
                rating_value = excluded.rating_value,
                review_count = excluded.review_count,
                reviews_per_month = excluded.reviews_per_month,
                instant_bookable = excluded.instant_bookable,
                minimum_nights = excluded.minimum_nights,
                maximum_nights = excluded.maximum_nights,
                last_scraped_at = excluded.last_scraped_at,
                is_active = 1,
                last_run_id = excluded.last_run_id
            """,
            (
                listing_id,
                data.get("url", f"https://www.airbnb.com/rooms/{listing_id}"),
                data.get("name"),
                data.get("description"),
                # "title" from search has format "Tiny home in Pujut"
                data.get("property_type") or data.get("type") or (data.get("title", "").split(" in ")[0] if data.get("title") else None),
                data.get("room_type"),
                lat,
                lng,
                zone_id,
                data.get("neighborhood") or (data.get("title", "").split(" in ")[-1] if " in " in data.get("title", "") else None),
                _safe_int(data.get("accommodates") or data.get("person_capacity")),
                _safe_int(data.get("bedrooms")),
                _safe_int(data.get("beds")),
                _safe_float(data.get("bathrooms")),
                # Price from search is nested: price.unit.amount
                _safe_float(
                    data.get("nightly_price")
                    or (data.get("price", {}).get("unit", {}).get("amount") if isinstance(data.get("price"), dict) else data.get("price"))
                ),
                _safe_float(
                    data.get("cleaning_fee")
                    or (data.get("fee", {}).get("cleaning", {}).get("amount") if isinstance(data.get("fee"), dict) else None)
                ),
                data.get("currency", AIRBNB_CURRENCY),
                str(data.get("host_id", "")) or None,
                data.get("host_name"),
                _bool_to_int(data.get("is_superhost") or data.get("superhost")),
                # Rating from search is nested: rating.value
                _safe_float(
                    data.get("rating_overall")
                    or (data.get("rating", {}).get("value") if isinstance(data.get("rating"), dict) else data.get("rating"))
                ),
                _safe_float(data.get("rating_accuracy")),
                _safe_float(data.get("rating_checkin")),
                _safe_float(data.get("rating_cleanliness")),
                _safe_float(data.get("rating_communication")),
                _safe_float(data.get("rating_location")),
                _safe_float(data.get("rating_value")),
                # Review count from search is nested: rating.reviewCount (as string)
                _safe_int(
                    data.get("review_count")
                    or data.get("reviews_count")
                    or (data.get("rating", {}).get("reviewCount") if isinstance(data.get("rating"), dict) else None)
                ),
                _safe_float(data.get("reviews_per_month")),
                _bool_to_int(data.get("instant_bookable")),
                _safe_int(data.get("minimum_nights")),
                _safe_int(data.get("maximum_nights")),
                now,  # first_scraped_at (ignored on conflict)
                now,  # last_scraped_at
                run_id,
            ),
        )
        return is_new

    def _upsert_amenities(self, listing_id: str, amenities: list[str]):
        """Insert amenities for a listing (ignoring duplicates)."""
        if not amenities:
            return
        conn = self._get_conn()
        conn.executemany(
            """
            INSERT OR IGNORE INTO airbnb_amenities (listing_id, amenity)
            VALUES (?, ?)
            """,
            [(listing_id, a.strip()) for a in amenities if a and a.strip()],
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=16),
        retry=retry_if_exception_type(sqlite3.OperationalError),
        before_sleep=before_sleep_log(logger, 20),
        reraise=True,
    )
    def _insert_calendar_snapshot(
        self,
        listing_id: str,
        run_id: int,
        snapshot_date: str,
        is_available: bool,
        price: float | None,
    ):
        """Insert a single calendar snapshot row."""
        conn = self._get_conn()
        conn.execute(
            """
            INSERT INTO calendar_snapshots
                (source, listing_id, run_id, snapshot_date, scraped_at,
                 is_available, price, currency)
            VALUES ('airbnb', ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (source, listing_id, snapshot_date, run_id) DO UPDATE SET
                is_available = excluded.is_available,
                price = excluded.price,
                scraped_at = excluded.scraped_at
            """,
            (
                listing_id,
                run_id,
                snapshot_date,
                now_iso(),
                _bool_to_int(is_available),
                _safe_float(price),
                AIRBNB_CURRENCY,
            ),
        )

    # ------------------------------------------------------------------
    # API wrappers with retry logic
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type(Exception),
        before_sleep=before_sleep_log(logger, 20),  # logging.INFO = 20
        reraise=True,
    )
    def _search_area(
        self, ne_lat: float, ne_long: float, sw_lat: float, sw_long: float
    ) -> list[dict]:
        """Search a bounding box with retry and rate limiting."""
        check_in, check_out = _check_in_out()
        rate_limit()
        results = pyairbnb.search_all(
            check_in=check_in,
            check_out=check_out,
            ne_lat=ne_lat,
            ne_long=ne_long,
            sw_lat=sw_lat,
            sw_long=sw_long,
            zoom_value=2,
            price_min=0,
            price_max=10000,
            currency=AIRBNB_CURRENCY,
            proxy_url=self.proxy_url,
        )
        return results if results else []

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type(Exception),
        before_sleep=before_sleep_log(logger, 20),
        reraise=True,
    )
    def _fetch_listing_details(self, listing_id: str) -> dict:
        """Fetch full listing details with retry and rate limiting."""
        check_in, check_out = _check_in_out()
        rate_limit()
        details = pyairbnb.get_details(
            room_id=int(listing_id),
            check_in=check_in,
            check_out=check_out,
            currency=AIRBNB_CURRENCY,
            proxy_url=self.proxy_url,
        )
        return details if details else {}

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type(Exception),
        before_sleep=before_sleep_log(logger, 20),
        reraise=True,
    )
    def _fetch_calendar(self, listing_id: str) -> list[dict]:
        """Fetch calendar data with retry and rate limiting."""
        rate_limit()
        api_key = self._get_api_key()
        cal = pyairbnb.get_calendar(
            api_key=api_key,
            room_id=listing_id,
            proxy_url=self.proxy_url,
        )
        return cal if cal else []

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def _search_zone_box(
        self, zone_id: str, ne_lat: float, ne_long: float, sw_lat: float, sw_long: float,
        depth: int = 0,
    ) -> dict[str, dict]:
        """Search a bounding box, subdividing if we hit the result cap.

        Returns a dict of {listing_id: listing_data} to auto-deduplicate.
        Max recursion depth of 3 prevents infinite subdivision.
        """
        max_depth = 3
        found: dict[str, dict] = {}

        try:
            results = self._search_area(ne_lat, ne_long, sw_lat, sw_long)
        except Exception as e:
            logger.error(
                "Search failed for zone %s box (%.4f,%.4f)-(%.4f,%.4f): %s",
                zone_id, sw_lat, sw_long, ne_lat, ne_long, e,
            )
            self._errors.append(f"Search failed for {zone_id}: {e}")
            return found

        logger.info(
            "Zone %s box (depth=%d): %d results from (%.4f,%.4f)-(%.4f,%.4f)",
            zone_id, depth, len(results), sw_lat, sw_long, ne_lat, ne_long,
        )

        for r in results:
            lid = str(r.get("room_id") or r.get("id") or "")
            if lid:
                found[lid] = r

        # If we hit the cap and have room to subdivide, split the box
        if len(results) >= self._RESULT_CAP and depth < max_depth:
            logger.info(
                "Zone %s hit result cap (%d). Subdividing (depth %d -> %d).",
                zone_id, len(results), depth, depth + 1,
            )
            sub_boxes = _subdivide_box(ne_lat, ne_long, sw_lat, sw_long)
            for box in sub_boxes:
                sub_results = self._search_zone_box(
                    zone_id,
                    box["ne_lat"], box["ne_long"],
                    box["sw_lat"], box["sw_long"],
                    depth=depth + 1,
                )
                found.update(sub_results)

        return found

    def discover_listings(self, run_id: int) -> list[str]:
        """Search all 8 Lombok zones and persist discovered listings.

        Returns a list of all discovered listing IDs.
        """
        all_listing_ids: list[str] = []
        seen_ids: set[str] = set()

        for zone_id, bounds in ZONE_BOUNDS.items():
            zone_name = ZONES.get(zone_id, {}).get("name", zone_id)
            logger.info("--- Discovering listings in zone: %s (%s) ---", zone_id, zone_name)

            zone_listings = self._search_zone_box(
                zone_id,
                ne_lat=bounds["lat_max"],
                ne_long=bounds["lng_max"],
                sw_lat=bounds["lat_min"],
                sw_long=bounds["lng_min"],
            )

            zone_new = 0
            for lid, data in zone_listings.items():
                if lid in seen_ids:
                    continue
                seen_ids.add(lid)

                try:
                    is_new = self._upsert_listing(data, run_id)
                    if is_new:
                        self._listings_new += 1
                        zone_new += 1

                    # Handle amenities if present in search results
                    amenities = data.get("amenities", [])
                    if amenities and isinstance(amenities, list):
                        self._upsert_amenities(lid, amenities)

                    self._listings_seen += 1
                    all_listing_ids.append(lid)

                except Exception as e:
                    logger.error("Failed to upsert listing %s: %s", lid, e)
                    self._errors.append(f"Upsert listing {lid}: {e}")

            self._get_conn().commit()
            logger.info(
                "Zone %s: %d listings found (%d new)",
                zone_id, len(zone_listings), zone_new,
            )

        # NOTE: Enrichment via get_details() is disabled due to a pyairbnb
        # Cookies bug on Python 3.14. Search data already provides name, price,
        # rating, coordinates, and review count — sufficient for MVP.
        # self._enrich_new_listings(run_id, all_listing_ids)

        logger.info(
            "Discovery complete: %d total listings across all zones (%d new)",
            self._listings_seen, self._listings_new,
        )
        return all_listing_ids

    def _enrich_new_listings(self, run_id: int, listing_ids: list[str]):
        """Fetch detailed info for listings that lack sub-ratings or amenities.

        Search results often include only basic data. The details endpoint
        gives us sub-ratings, full amenity lists, and more accurate pricing.
        We only enrich listings that have no sub-ratings yet to avoid
        unnecessary API calls on re-scrapes.
        """
        conn = self._get_conn()
        rows = conn.execute(
            """
            SELECT listing_id FROM airbnb_listings
            WHERE listing_id IN ({})
              AND last_run_id = ?
              AND rating_accuracy IS NULL
              AND review_count > 0
            """.format(",".join("?" * len(listing_ids))),
            listing_ids + [run_id],
        ).fetchall()

        ids_to_enrich = [row[0] for row in rows] if rows else []
        if not ids_to_enrich:
            logger.info("No listings need detail enrichment.")
            return

        logger.info("Enriching %d listings with detailed data...", len(ids_to_enrich))

        for i, lid in enumerate(ids_to_enrich, 1):
            try:
                details = self._fetch_listing_details(lid)
                if not details:
                    continue

                # Update the listing with richer data
                self._upsert_listing(details, run_id)

                # Upsert amenities from details
                amenities = details.get("amenities", [])
                if amenities and isinstance(amenities, list):
                    self._upsert_amenities(lid, amenities)

                if i % 25 == 0:
                    conn.commit()
                    logger.info("Enriched %d / %d listings", i, len(ids_to_enrich))

            except Exception as e:
                logger.warning("Failed to enrich listing %s: %s", lid, e)
                self._errors.append(f"Enrich listing {lid}: {e}")

        conn.commit()
        logger.info("Enrichment complete for %d listings.", len(ids_to_enrich))

    # ------------------------------------------------------------------
    # Calendar scraping
    # ------------------------------------------------------------------

    def scrape_calendars(self, run_id: int, listing_ids: list[str]):
        """Fetch and persist calendar availability for each listing.

        Scrapes up to CALENDAR_DAYS_FORWARD days of availability for
        every listing in the provided list.
        """
        total = len(listing_ids)
        cutoff = date.today() + timedelta(days=CALENDAR_DAYS_FORWARD)
        logger.info(
            "Scraping calendars for %d listings (up to %s)...",
            total, cutoff.isoformat(),
        )

        conn = self._get_conn()

        # Cache listing prices for fallback (pyairbnb calendar returns null prices)
        price_cache: dict[str, float | None] = {}

        for i, lid in enumerate(listing_ids, 1):
            try:
                # Resume logic: skip listings already scraped in this run
                already = conn.execute(
                    "SELECT COUNT(*) FROM calendar_snapshots WHERE source='airbnb' AND listing_id=? AND run_id=?",
                    (lid, run_id),
                ).fetchone()[0]
                if already > 0:
                    logger.debug("Skipping listing %s (already has %d snapshots in run %d)", lid, already, run_id)
                    continue

                cal_data = self._fetch_calendar(lid)
                if not cal_data:
                    logger.debug("No calendar data for listing %s", lid)
                    continue

                count = 0
                # Calendar returns list of month dicts, each with a "days" list
                for month_block in cal_data:
                    days = month_block.get("days", [])
                    if not days:
                        # Might be a flat list of day dicts (fallback)
                        if "calendarDate" in month_block or "date" in month_block:
                            days = [month_block]
                        else:
                            continue

                    for day in days:
                        day_date = day.get("calendarDate") or day.get("date", "")
                        if not day_date:
                            continue

                        try:
                            parsed = date.fromisoformat(day_date)
                        except ValueError:
                            continue

                        if parsed < date.today() or parsed > cutoff:
                            continue

                        is_available = bool(day.get("available", False))
                        # Price: try calendar data first
                        price_data = day.get("price", {})
                        if isinstance(price_data, dict):
                            price = _safe_float(price_data.get("localPriceFormatted") or price_data.get("amount"))
                        else:
                            price = _safe_float(price_data)
                        # Fallback: pyairbnb calendar returns null prices,
                        # so use listing's base nightly_price from search results
                        if price is None:
                            if lid not in price_cache:
                                row = conn.execute(
                                    "SELECT nightly_price FROM airbnb_listings WHERE listing_id=?", (lid,)
                                ).fetchone()
                                price_cache[lid] = row[0] if row else None
                            price = price_cache[lid]

                        self._insert_calendar_snapshot(lid, run_id, day_date, is_available, price)
                        count += 1

                self._snapshots_added += count

                # Commit in batches for performance
                if i % 10 == 0:
                    conn.commit()
                    logger.info(
                        "Calendars scraped: %d / %d listings (%d snapshots so far)",
                        i, total, self._snapshots_added,
                    )

            except Exception as e:
                logger.error("Calendar scrape failed for listing %s: %s", lid, e)
                self._errors.append(f"Calendar {lid}: {e}")

        conn.commit()
        logger.info(
            "Calendar scraping complete: %d snapshots from %d listings",
            self._snapshots_added, total,
        )

    # ------------------------------------------------------------------
    # Price history recording
    # ------------------------------------------------------------------

    def _record_price_history(self, run_id: int, listing_ids: list[str]):
        """Snapshot current base prices into price_history for trend analysis."""
        conn = self._get_conn()
        placeholders = ",".join("?" * len(listing_ids))
        rows = conn.execute(
            f"""
            SELECT listing_id, nightly_price, cleaning_fee, currency
            FROM airbnb_listings
            WHERE listing_id IN ({placeholders})
              AND nightly_price IS NOT NULL
            """,
            listing_ids,
        ).fetchall()

        if not rows:
            return

        conn.executemany(
            """
            INSERT INTO price_history
                (source, listing_id, run_id, recorded_at, nightly_price, cleaning_fee, currency)
            VALUES ('airbnb', ?, ?, ?, ?, ?, ?)
            """,
            [
                (row[0], run_id, now_iso(), row[1], row[2], row[3])
                for row in rows
            ],
        )
        conn.commit()
        logger.info("Recorded price history for %d listings.", len(rows))

    # ------------------------------------------------------------------
    # Main orchestration
    # ------------------------------------------------------------------

    def run(self, run_type: str = "full") -> int:
        """Execute a complete scrape: discover listings, scrape calendars.

        Args:
            run_type: One of 'full', 'incremental', or 'calendar_only'.
                - full: discover all listings + scrape all calendars.
                - incremental: discover listings + scrape calendars for new only.
                - calendar_only: skip discovery, scrape calendars for existing listings.

        Returns:
            The run_id for this scrape session.
        """
        # Reset counters
        self._listings_seen = 0
        self._listings_new = 0
        self._snapshots_added = 0
        self._errors = []

        run_id = self._start_run(run_type)

        try:
            if run_type == "calendar_only":
                # Scrape calendars for all known active listings
                conn = self._get_conn()
                rows = conn.execute(
                    "SELECT listing_id FROM airbnb_listings WHERE is_active = 1"
                ).fetchall()
                listing_ids = [row[0] for row in rows]
                logger.info(
                    "Calendar-only run: %d active listings to scrape.", len(listing_ids)
                )
            else:
                # Discover listings across all zones
                listing_ids = self.discover_listings(run_id)

                if run_type == "incremental":
                    # For incremental, only scrape calendars for newly found listings
                    conn = self._get_conn()
                    rows = conn.execute(
                        """
                        SELECT listing_id FROM airbnb_listings
                        WHERE last_run_id = ? AND first_scraped_at = last_scraped_at
                        """,
                        (run_id,),
                    ).fetchall()
                    listing_ids = [row[0] for row in rows]
                    logger.info(
                        "Incremental run: %d new listings to scrape calendars for.",
                        len(listing_ids),
                    )

            # Scrape calendars
            if listing_ids:
                self.scrape_calendars(run_id, listing_ids)
                self._record_price_history(run_id, listing_ids)
            else:
                logger.warning("No listings to scrape calendars for.")

            # Determine final status
            if self._snapshots_added == 0 and self._listings_seen == 0:
                status = "failed"
                error_msg = "No listings found and no snapshots collected"
            elif self._snapshots_added == 0:
                status = "partial"
                error_msg = f"Found {self._listings_seen} listings but collected 0 snapshots"
                if self._errors:
                    error_msg += "; " + "; ".join(self._errors[:20])
            elif self._errors:
                status = "partial"
                error_msg = "; ".join(self._errors[:20])
            else:
                status = "completed"
                error_msg = None
            self._finish_run(run_id, status=status, error_message=error_msg)

        except KeyboardInterrupt:
            logger.warning("Run %d interrupted by user.", run_id)
            self._finish_run(run_id, status="partial", error_message="Interrupted by user")
            raise

        except Exception as e:
            logger.exception("Run %d failed with unhandled error: %s", run_id, e)
            self._finish_run(run_id, status="failed", error_message=str(e))
            raise

        finally:
            if self.conn:
                self.conn.close()
                self.conn = None

        return run_id


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Airbnb scraper for Lombok Market Intelligence",
    )
    parser.add_argument(
        "--run-type",
        choices=["full", "incremental", "calendar_only"],
        default="full",
        help="Type of scrape run (default: full)",
    )
    parser.add_argument(
        "--proxy",
        default=None,
        help="SOCKS5 or HTTP proxy URL for pyairbnb requests",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="Path to SQLite database (defaults to data/lombok_intel.db)",
    )
    args = parser.parse_args()

    from pathlib import Path

    db_path = Path(args.db) if args.db else None

    scraper = AirbnbScraper(db_path=db_path, proxy_url=args.proxy)
    run_id = scraper.run(run_type=args.run_type)

    print(f"\nScrape run {run_id} finished.")
    print(f"  Listings seen:    {scraper._listings_seen}")
    print(f"  Listings new:     {scraper._listings_new}")
    print(f"  Calendar entries: {scraper._snapshots_added}")
    if scraper._errors:
        print(f"  Errors:           {len(scraper._errors)}")
