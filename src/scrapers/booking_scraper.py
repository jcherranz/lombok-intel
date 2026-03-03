"""Playwright-based Booking.com scraper for Lombok Market Intelligence."""
from __future__ import annotations
import html as html_lib
import random
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from playwright.sync_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
from src.config import BOOKING_CURRENCY, BOOKING_LANGUAGE, CALENDAR_DAYS_FORWARD, ZONE_BOUNDS, ZONES
from src.db.init_db import DB_PATH, get_connection
from src.utils import assign_zone, now_iso, rate_limit, setup_logger, validate_coordinates, validate_price
SEARCH_URL_BASE = "https://www.booking.com/searchresults.html"
_USER_AGENTS = [
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
     "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36", 3),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
     "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36", 2),
    ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
     "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36", 2),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) "
     "Gecko/20100101 Firefox/133.0", 1),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
     "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15", 1),
    ("Mozilla/5.0 (X11; Linux x86_64; rv:133.0) "
     "Gecko/20100101 Firefox/133.0", 1),
]
_UA_POOL = [ua for ua, weight in _USER_AGENTS for _ in range(weight)]
CONTEXT_ROTATION_INTERVAL = 50  # rotate browser context every N page loads
PAGE_TIMEOUT_MS = 60_000
WAF_SETTLE_MS = 5_000
RESULTS_PER_PAGE = 25
MAX_PAGES_PER_ZONE = 12
PROPERTY_CARD_RE = re.compile(r'data-testid="property-card"')
PROPERTY_ID_PATTERNS = [
    re.compile(r'data-hotelid=["\'](\d+)["\']'),
    re.compile(r'"hotel_id"\s*:\s*"?(\d+)"?'),
    re.compile(r'b_hotel_id["\']?\s*[:=]\s*"?(\d+)"?'),
    re.compile(r'[?&]hotel_id=(\d+)'),
    re.compile(r'sr_(?:pri_)?blocks=(\d+)_'),  # Booking.com 2026 format
]
COORD_PAIR_PATTERNS = [
    re.compile(
        r"b_map_center_latitude\s*[:=]\s*['\"]?(-?\d+(?:\.\d+)?)['\"]?.{0,200}?"
        r"b_map_center_longitude\s*[:=]\s*['\"]?(-?\d+(?:\.\d+)?)",
        re.DOTALL,
    ),
    re.compile(r'"latitude"\s*:\s*(-?\d+(?:\.\d+)?)\s*,\s*"longitude"\s*:\s*(-?\d+(?:\.\d+)?)'),
]
logger = setup_logger("booking_scraper")
@dataclass
class BookingProperty:
    property_id: str
    name: str
    url: str
    property_type: str | None = None
    star_rating: int | None = None
    latitude: float | None = None
    longitude: float | None = None
    review_score: float | None = None
    review_count: int | None = None
    zone_id: str | None = None
@dataclass
class RoomType:
    property_id: str
    room_name: str
    nightly_price: float | None = None
    currency: str = BOOKING_CURRENCY
    max_occupancy: int | None = None
    bed_type: str | None = None
@dataclass
class AvailabilitySnapshot:
    property_id: str
    snapshot_date: str
    is_available: bool
    price: float | None = None
    currency: str | None = BOOKING_CURRENCY
    available_rooms: int | None = None
class BookingScraper:
    def __init__(self, db_path: Path | None = None):
        self.db_path = db_path or DB_PATH
        self.conn: sqlite3.Connection | None = None
        self.run_id: int | None = None
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None
        self._listings_seen = 0
        self._listings_new = 0
        self._snapshots_added = 0
        self._page_load_count = 0

    def _pick_ua(self) -> str:
        return random.choice(_UA_POOL)

    def _start_browser(self):
        from playwright_stealth import stealth_sync
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(headless=True)
        self._context = self._browser.new_context(
            user_agent=self._pick_ua(),
            locale="en-US",
            viewport={"width": 1920, "height": 1080},
        )
        self._page = self._context.new_page()
        stealth_sync(self._page)
        # Block images, CSS, fonts to reduce memory footprint
        self._page.route("**/*.{png,jpg,jpeg,gif,svg,webp,css,woff,woff2,ttf,eot}",
                         lambda route: route.abort())
    def _stop_browser(self):
        try:
            if self._page and not self._page.is_closed():
                self._page.close()
        except Exception:
            pass
        try:
            if self._context:
                self._context.close()
        except Exception:
            pass
        try:
            if self._browser:
                self._browser.close()
        except Exception:
            pass
        try:
            if self._playwright:
                self._playwright.stop()
        except Exception:
            pass
        self._page = None
        self._context = None
        self._browser = None
        self._playwright = None
    def _booking_delay(self):
        rate_limit()
        time.sleep(random.uniform(1.0, 2.0))
    def _rotate_context(self):
        """Close current context and open a fresh one to prevent memory leaks."""
        from playwright_stealth import stealth_sync
        try:
            if self._page and not self._page.is_closed():
                self._page.close()
        except Exception:
            pass
        try:
            if self._context:
                self._context.close()
        except Exception:
            pass
        self._context = self._browser.new_context(
            user_agent=self._pick_ua(),
            locale="en-US",
            viewport={"width": 1920, "height": 1080},
        )
        self._page = self._context.new_page()
        stealth_sync(self._page)
        self._page.route("**/*.{png,jpg,jpeg,gif,svg,webp,css,woff,woff2,ttf,eot}",
                         lambda route: route.abort())
        logger.info("Browser context rotated (after %d page loads)", self._page_load_count)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((PlaywrightTimeoutError, RuntimeError)),
        before_sleep=before_sleep_log(logger, 20),
        reraise=True,
    )
    def _load_page_html(self, url: str, waf_wait_ms: int = WAF_SETTLE_MS) -> str:
        # Rotate context every N page loads to prevent memory leaks
        if self._page_load_count > 0 and self._page_load_count % CONTEXT_ROTATION_INTERVAL == 0:
            self._rotate_context()
        if not self._page or self._page.is_closed():
            if not self._context:
                raise RuntimeError("Playwright context is not initialized")
            self._page = self._context.new_page()
        self._booking_delay()
        try:
            self._page.goto(url, timeout=PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
            self._page.wait_for_timeout(waf_wait_ms)
            self._page_load_count += 1
            return self._page.content()
        except PlaywrightTimeoutError as exc:
            raise RuntimeError(f"Timeout loading {url}") from exc
    @staticmethod
    def _zone_name(zone_id: str) -> str:
        zone = ZONES.get(zone_id, {})
        return str(zone.get("name")) if isinstance(zone, dict) and zone.get("name") else zone_id
    @staticmethod
    def _clean_text(raw: str | None) -> str:
        if not raw:
            return ""
        text = re.sub(r"<[^>]+>", " ", raw)
        text = html_lib.unescape(text)
        return re.sub(r"\s+", " ", text).strip()
    @staticmethod
    def _to_float(token: str | None) -> float | None:
        if not token:
            return None
        try:
            return float(token.replace(",", "").strip())
        except ValueError:
            return None
    @staticmethod
    def _search_url(search_term: str, checkin: str, checkout: str, offset: int = 0) -> str:
        params = {"ss": search_term, "checkin": checkin, "checkout": checkout, "selected_currency": BOOKING_CURRENCY, "lang": BOOKING_LANGUAGE, **({"offset": offset} if offset else {})}
        return f"{SEARCH_URL_BASE}?{urlencode(params)}"
    @staticmethod
    def _extract_property_cards(page_html: str) -> list[str]:
        starts = [m.start() for m in PROPERTY_CARD_RE.finditer(page_html)]
        if not starts:
            return []
        return [page_html[start:(starts[idx + 1] if idx + 1 < len(starts) else len(page_html))] for idx, start in enumerate(starts)]
    def _extract_property_id(self, card_html: str) -> str | None:
        for pattern in PROPERTY_ID_PATTERNS:
            match = pattern.search(card_html)
            if match:
                return match.group(1)
        return None
    def _extract_price(self, card_html: str) -> float | None:
        context = ""
        for pattern in [
            r'data-testid="price-and-discounted-price"[^>]*>(.*?)</',
            r'data-testid="price-for-x-nights"[^>]*>(.*?)</',
            r'data-testid="[^"]*price[^"]*"[^>]*>(.*?)</',
        ]:
            match = re.search(pattern, card_html, re.DOTALL)
            if match:
                context = match.group(1)
                break
        text = self._clean_text(context or card_html)
        money = re.search(r"(?:US\$|USD|IDR|Rp|\$)\s*([\d.,]+)", text, re.IGNORECASE)
        if money:
            return self._to_float(money.group(1))
        fallback = re.search(r"\b(\d{2,}(?:[,.]\d{3})*(?:\.\d{2})?)\b", text)
        return self._to_float(fallback.group(1) if fallback else None)
    def _parse_property_card(self, card_html: str) -> tuple[BookingProperty | None, list[RoomType]]:
        prop_id = self._extract_property_id(card_html)
        if not prop_id:
            return None, []
        name_match = re.search(r'data-testid="title"[^>]*>(.*?)</', card_html, re.DOTALL)
        if not name_match:
            name_match = re.search(r'data-testid="title-link"[^>]*aria-label="([^"]+)"', card_html)
        name = self._clean_text(name_match.group(1) if name_match else None) or f"Property {prop_id}"
        href_match = re.search(r'href="([^"]*?/hotel/[^"]+)"', card_html)
        raw_href = html_lib.unescape(href_match.group(1)) if href_match else ""
        if raw_href.startswith("/"):
            url = f"https://www.booking.com{raw_href}"
        elif raw_href.startswith("http"):
            url = raw_href
        else:
            url = f"https://www.booking.com/hotel/id/{prop_id}.html"
        url = url.split("?")[0]
        review_score = None
        score_ctx = re.search(r'data-testid="review-score"[^>]*>(.*?)</div>', card_html, re.DOTALL)
        if score_ctx:
            for num in re.findall(r"\d+(?:\.\d+)?", self._clean_text(score_ctx.group(1))):
                value = self._to_float(num)
                if value is not None and 0 < value <= 10:
                    review_score = value
                    break
        review_count = None
        count_match = re.search(r"(\d[\d,]*)\s+reviews?", card_html, re.IGNORECASE)
        if count_match:
            try:
                review_count = int(count_match.group(1).replace(",", ""))
            except ValueError:
                review_count = None
        type_match = re.search(r'data-testid="(?:property-card-unit-configuration|property-type-badge)"[^>]*>(.*?)</', card_html, re.DOTALL)
        property_type = self._clean_text(type_match.group(1)) if type_match else None
        star_match = re.search(r"(\d)\s*(?:out of 5|star)", card_html, re.IGNORECASE)
        star_rating = int(star_match.group(1)) if star_match else None
        price = self._extract_price(card_html)
        validate_price(price)
        rooms = []
        if price is not None:
            rooms.append(RoomType(property_id=prop_id, room_name="Standard Room", nightly_price=price, currency=BOOKING_CURRENCY))
        prop = BookingProperty(
            property_id=prop_id,
            name=name,
            url=url,
            property_type=property_type,
            star_rating=star_rating,
            review_score=review_score,
            review_count=review_count,
        )
        return prop, rooms
    def _extract_coords_from_html(self, page_html: str) -> tuple[float | None, float | None]:
        for pattern in COORD_PAIR_PATTERNS:
            match = pattern.search(page_html)
            if match:
                lat = self._to_float(match.group(1))
                lng = self._to_float(match.group(2))
                if lat is not None and lng is not None:
                    return lat, lng
        lat_match = re.search(r"b_map_center_latitude\s*[:=]\s*['\"]?(-?\d+(?:\.\d+)?)", page_html)
        lng_match = re.search(r"b_map_center_longitude\s*[:=]\s*['\"]?(-?\d+(?:\.\d+)?)", page_html)
        return self._to_float(lat_match.group(1) if lat_match else None), self._to_float(lng_match.group(1) if lng_match else None)
    def _scan_zone(
        self,
        zone_id: str,
        checkin: str,
        checkout: str,
        target_ids: set[str] | None = None,
    ) -> dict[str, tuple[BookingProperty, list[RoomType]]]:
        zone_name = self._zone_name(zone_id)
        search_term = f"{zone_name}, Lombok, Indonesia"
        found: dict[str, tuple[BookingProperty, list[RoomType]]] = {}
        seen_pages: set[tuple[str, ...]] = set()
        for page_idx in range(MAX_PAGES_PER_ZONE):
            url = self._search_url(search_term, checkin, checkout, offset=page_idx * RESULTS_PER_PAGE)
            cards = self._extract_property_cards(self._load_page_html(url))
            if not cards:
                break
            page_ids: list[str] = []
            for card in cards:
                prop, rooms = self._parse_property_card(card)
                if not prop:
                    continue
                page_ids.append(prop.property_id)
                if target_ids and prop.property_id not in target_ids:
                    continue
                if prop.property_id in found:
                    continue
                found[prop.property_id] = (prop, rooms)
            if target_ids and target_ids.issubset(found.keys()):
                break
            if not page_ids:
                break
            signature = tuple(page_ids[:5])
            if signature in seen_pages:
                break
            seen_pages.add(signature)
            if len(cards) < RESULTS_PER_PAGE:
                break
        return found
    def _discover_zone(self, zone_id: str, checkin: str, checkout: str) -> list[tuple[BookingProperty, list[RoomType]]]:
        results = list(self._scan_zone(zone_id, checkin, checkout).values())
        for prop, _ in results:
            prop.zone_id = zone_id
        logger.info("Zone %s (%s): discovered %d properties", zone_id, self._zone_name(zone_id), len(results))
        return results
    def _hydrate_property_coordinates(self, properties: list[tuple[BookingProperty, list[RoomType]]]):
        for prop, _ in properties:
            try:
                html = self._load_page_html(prop.url, waf_wait_ms=2_000)
                lat, lng = self._extract_coords_from_html(html)
                if lat is None or lng is None:
                    logger.warning("Coordinates not found for property %s", prop.property_id)
                    continue
                prop.latitude = lat
                prop.longitude = lng
                prop.zone_id = assign_zone(lat, lng) or prop.zone_id
            except Exception as exc:
                logger.warning("Failed coordinate enrichment for %s: %s", prop.property_id, exc)
    def discover_properties(self, checkin: str, checkout: str) -> list[tuple[BookingProperty, list[RoomType]]]:
        all_props: dict[str, tuple[BookingProperty, list[RoomType]]] = {}
        for zone_id in ZONE_BOUNDS:
            logger.info("--- Searching zone %s (%s) ---", zone_id, self._zone_name(zone_id))
            try:
                for prop, rooms in self._discover_zone(zone_id, checkin, checkout):
                    existing = all_props.get(prop.property_id)
                    if not existing:
                        all_props[prop.property_id] = (prop, rooms)
                    elif not existing[1] and rooms:
                        all_props[prop.property_id] = (existing[0], rooms)
            except Exception as exc:
                logger.error("Zone search failed for %s: %s", zone_id, exc)
        properties = list(all_props.values())
        logger.info("Discovered %d unique properties before coordinate enrichment", len(properties))
        self._hydrate_property_coordinates(properties)
        return properties
    def _scan_zone_availability(self, zone_id: str, checkin: str, checkout: str, target_ids: set[str]) -> dict[str, tuple[float | None, int | None]]:
        if not target_ids:
            return {}
        found = self._scan_zone(zone_id, checkin, checkout, target_ids=target_ids)
        return {prop_id: (min((r.nightly_price for r in rooms if r.nightly_price is not None), default=None), len(rooms) if rooms else None) for prop_id, (_, rooms) in found.items()}
    def scrape_availability(self, properties: list[tuple[BookingProperty, list[RoomType]]], days_forward: int = CALENDAR_DAYS_FORWARD) -> list[AvailabilitySnapshot]:
        snapshots: list[AvailabilitySnapshot] = []
        if not properties:
            return snapshots
        property_ids = [prop.property_id for prop, _ in properties]
        zone_targets = {zone_id: set() for zone_id in ZONE_BOUNDS}
        unassigned: set[str] = set()
        for prop, _ in properties:
            if prop.zone_id in zone_targets:
                zone_targets[prop.zone_id].add(prop.property_id)
            else:
                unassigned.add(prop.property_id)
        today = date.today()
        for day_offset in range(days_forward):
            checkin = (today + timedelta(days=day_offset)).isoformat()
            checkout = (today + timedelta(days=day_offset + 1)).isoformat()
            logger.info("Availability scan %d/%d: %s", day_offset + 1, days_forward, checkin)
            day_available: dict[str, tuple[float | None, int | None]] = {}
            failed_zone_ids: set[str] = set()
            remaining_unassigned = set(unassigned)
            for zone_id in ZONE_BOUNDS:
                target_ids = set(zone_targets.get(zone_id, set())) | remaining_unassigned
                if not target_ids:
                    continue
                try:
                    zone_found = self._scan_zone_availability(zone_id, checkin, checkout, target_ids)
                    day_available.update(zone_found)
                    remaining_unassigned.difference_update(zone_found.keys())
                except Exception as exc:
                    logger.warning("Availability scan failed for zone %s on %s: %s", zone_id, checkin, exc)
                    failed_zone_ids.add(zone_id)
            # Build set of property IDs in failed zones — skip writing
            # snapshots for them since we don't know their actual state.
            # Also skip unassigned properties if any zone failed (they get
            # scanned across all zones).
            skip_ids = set()
            if failed_zone_ids:
                for zid in failed_zone_ids:
                    skip_ids.update(zone_targets.get(zid, set()))
                if remaining_unassigned:
                    skip_ids.update(unassigned)
                logger.warning("Skipping %d properties in failed zones: %s", len(skip_ids), failed_zone_ids)
            for property_id in property_ids:
                if property_id in skip_ids:
                    continue
                hit = day_available.get(property_id)
                snapshots.append(
                    AvailabilitySnapshot(
                        property_id=property_id,
                        snapshot_date=checkin,
                        is_available=hit is not None,
                        price=hit[0] if hit else None,
                        currency=BOOKING_CURRENCY,
                        available_rooms=hit[1] if hit else 0,
                    )
                )
        logger.info("Collected %d availability snapshots", len(snapshots))
        return snapshots
    def _create_scrape_run(self, run_type: str = "full") -> int:
        cursor = self.conn.execute("INSERT INTO scrape_runs (source, run_type, started_at, status) VALUES ('booking', ?, ?, 'running')", (run_type, now_iso()))
        self.conn.commit()
        run_id = cursor.lastrowid
        logger.info("Created scrape run #%d (type=%s)", run_id, run_type)
        return run_id
    def _update_scrape_run(self, status: str, error_message: str | None = None):
        self.conn.execute(
            "UPDATE scrape_runs SET finished_at = ?, status = ?, listings_seen = ?, listings_new = ?, snapshots_added = ?, error_message = ? WHERE run_id = ?",
            (now_iso(), status, self._listings_seen, self._listings_new, self._snapshots_added, error_message, self.run_id),
        )
        self.conn.commit()
    def _upsert_property(self, prop: BookingProperty):
        validate_coordinates(prop.latitude, prop.longitude)
        self._listings_seen += 1
        existing = self.conn.execute("SELECT property_id FROM booking_listings WHERE property_id = ?", (prop.property_id,)).fetchone()
        if existing:
            self.conn.execute(
                "UPDATE booking_listings SET url = ?, name = ?, property_type = COALESCE(?, property_type), star_rating = COALESCE(?, star_rating), latitude = COALESCE(?, latitude), longitude = COALESCE(?, longitude), zone_id = COALESCE(?, zone_id), review_score = COALESCE(?, review_score), review_count = COALESCE(?, review_count), last_scraped_at = ?, is_active = 1, last_run_id = ? WHERE property_id = ?",
                (prop.url, prop.name, prop.property_type, prop.star_rating, prop.latitude, prop.longitude, prop.zone_id, prop.review_score, prop.review_count, now_iso(), self.run_id, prop.property_id),
            )
        else:
            self._listings_new += 1
            self.conn.execute(
                "INSERT INTO booking_listings (property_id, url, name, property_type, star_rating, latitude, longitude, zone_id, review_score, review_count, first_scraped_at, last_scraped_at, is_active, last_run_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)",
                (prop.property_id, prop.url, prop.name, prop.property_type, prop.star_rating, prop.latitude, prop.longitude, prop.zone_id, prop.review_score, prop.review_count, now_iso(), now_iso(), self.run_id),
            )
    def _upsert_room_types(self, rooms: list[RoomType]):
        for room in rooms:
            self.conn.execute(
                "INSERT INTO booking_room_types (property_id, room_name, max_occupancy, bed_type, is_active, first_seen_at, last_seen_at) VALUES (?, ?, ?, ?, 1, ?, ?) ON CONFLICT (property_id, room_name) DO UPDATE SET max_occupancy = COALESCE(excluded.max_occupancy, max_occupancy), bed_type = COALESCE(excluded.bed_type, bed_type), is_active = 1, last_seen_at = excluded.last_seen_at",
                (room.property_id, room.room_name, room.max_occupancy, room.bed_type, now_iso(), now_iso()),
            )
    def _insert_amenities(self, property_id: str, amenities: list[str]):
        for amenity in amenities:
            self.conn.execute("INSERT OR IGNORE INTO booking_amenities (property_id, amenity) VALUES (?, ?)", (property_id, amenity))
    def _insert_snapshots(self, snapshots: list[AvailabilitySnapshot]):
        rows = [
            ("booking", snap.property_id, self.run_id, snap.snapshot_date, now_iso(), 1 if snap.is_available else 0, snap.price, snap.currency, snap.available_rooms)
            for snap in snapshots
        ]
        self.conn.executemany(
            "INSERT OR IGNORE INTO calendar_snapshots (source, listing_id, run_id, snapshot_date, scraped_at, is_available, price, currency, available_rooms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        self._snapshots_added += len(rows)
        logger.info("Inserted %d calendar snapshots", len(rows))
    def _insert_price_history(self, rooms: list[RoomType]):
        for room in rooms:
            if room.nightly_price is None:
                continue
            row = self.conn.execute("SELECT room_type_id FROM booking_room_types WHERE property_id = ? AND room_name = ?", (room.property_id, room.room_name)).fetchone()
            room_type_id = row[0] if row else None
            self.conn.execute(
                "INSERT INTO price_history (source, listing_id, room_type_id, run_id, recorded_at, nightly_price, currency) VALUES ('booking', ?, ?, ?, ?, ?, ?)",
                (room.property_id, room_type_id, self.run_id, now_iso(), room.nightly_price, room.currency),
            )
    def run(self, run_type: str = "full") -> dict[str, Any]:
        logger.info("Starting Booking.com scrape run (type=%s)", run_type)
        self.conn = get_connection(self.db_path)
        self.run_id = self._create_scrape_run(run_type)
        self._listings_seen = 0
        self._listings_new = 0
        self._snapshots_added = 0
        error_message = None
        status = "completed"
        try:
            self._start_browser()
            today = date.today()
            checkin = (today + timedelta(days=1)).isoformat()
            checkout = (today + timedelta(days=2)).isoformat()
            properties = self.discover_properties(checkin, checkout)
            if not properties:
                logger.warning("No properties discovered.")
                status = "partial"
            logger.info("Persisting %d properties to database...", len(properties))
            for idx, (prop, rooms) in enumerate(properties, start=1):
                for attempt in range(5):
                    try:
                        self._upsert_property(prop)
                        if rooms:
                            self._upsert_room_types(rooms)
                            self._insert_price_history(rooms)
                        break
                    except sqlite3.OperationalError as exc:
                        if "locked" in str(exc) and attempt < 4:
                            import time
                            wait = 2 ** attempt
                            logger.warning("DB locked persisting %s, retry %d/4 in %ds", prop.property_id, attempt + 1, wait)
                            time.sleep(wait)
                        else:
                            logger.error("Failed to persist property %s: %s", prop.property_id, exc)
                            break
                    except Exception as exc:
                        logger.error("Failed to persist property %s: %s", prop.property_id, exc)
                        break
                if idx % 10 == 0:
                    self.conn.commit()
            self.conn.commit()
            logger.info("Collecting %d-day availability snapshots...", CALENDAR_DAYS_FORWARD)
            snapshots = self.scrape_availability(properties, days_forward=CALENDAR_DAYS_FORWARD)
            if snapshots:
                self._insert_snapshots(snapshots)
                self.conn.commit()
        except KeyboardInterrupt:
            logger.warning("Scrape interrupted by user.")
            status = "partial"
            error_message = "Interrupted by user"
        except Exception as exc:
            logger.error("Scrape run failed: %s", exc, exc_info=True)
            status = "failed"
            error_message = str(exc)[:500]
        finally:
            try:
                if self.conn and self.run_id is not None:
                    self._update_scrape_run(status, error_message)
            finally:
                self._stop_browser()
                if self.conn:
                    self.conn.close()
                    self.conn = None
        summary = {
            "run_id": self.run_id,
            "status": status,
            "listings_seen": self._listings_seen,
            "listings_new": self._listings_new,
            "snapshots_added": self._snapshots_added,
            "error_message": error_message,
        }
        logger.info("=" * 60)
        logger.info("Scrape run #%d complete: %s", self.run_id, status)
        logger.info("  Listings seen: %d | New: %d | Snapshots: %d", self._listings_seen, self._listings_new, self._snapshots_added)
        logger.info("=" * 60)
        return summary
if __name__ == "__main__":
    import sys
    summary = BookingScraper().run()
    if summary.get("status") == "failed":
        print(f"Booking scraper FAILED: {summary.get('error_message', 'unknown')}")
        sys.exit(1)
