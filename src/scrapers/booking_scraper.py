"""Booking.com scraper for Lombok Market Intelligence.

Collects property listings, room types, and availability/pricing data
for Lombok, Indonesia from Booking.com's search results.

Primary approach: hit Booking.com's internal GraphQL endpoint (FullSearch).
Fallback approach: parse search results HTML when GraphQL fails or changes.

Usage:
    python -m src.scrapers.booking_scraper
"""

from __future__ import annotations

import json
import random
import re
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import quote_plus, urlencode

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from src.config import (
    BOOKING_CURRENCY,
    BOOKING_LANGUAGE,
    DB_PATH,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    ZONES,
)
from src.db.init_db import get_connection
from src.utils import assign_zone, now_iso, rate_limit, setup_logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GRAPHQL_URL = f"https://www.booking.com/dml/graphql?lang={BOOKING_LANGUAGE}"
SEARCH_URL_BASE = "https://www.booking.com/searchresults.html"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) "
    "Gecko/20100101 Firefox/134.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/18.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:134.0) "
    "Gecko/20100101 Firefox/134.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 OPR/115.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
]

# Regex patterns used across both GraphQL and HTML fallback parsing
CSRF_PATTERN = re.compile(r"b_csrf_token:\s*'(.+?)'")
PROPERTY_ID_PATTERN = re.compile(r"data-hotelid=[\"'](\d+)[\"']")
PROPERTY_NAME_PATTERN = re.compile(
    r'data-testid="title"[^>]*>([^<]+)<'
)
REVIEW_SCORE_PATTERN = re.compile(
    r'data-testid="review-score"[^>]*>.*?'
    r'<div[^>]*class="[^"]*"[^>]*>\s*([\d.]+)\s*</div>',
    re.DOTALL,
)
PRICE_PATTERN = re.compile(
    r'data-testid="price-and-discounted-price"[^>]*>\s*'
    r'(?:US\$|USD\s*|)\s*([\d,]+)',
    re.DOTALL,
)
REVIEW_COUNT_PATTERN = re.compile(
    r'(\d[\d,]*)\s+reviews?',
    re.IGNORECASE,
)

# Maximum pages to paginate through per zone search
MAX_PAGES_PER_ZONE = 10

# Number of days forward to check availability
AVAILABILITY_DAYS_FORWARD = 90

logger = setup_logger("booking_scraper")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class BookingProperty:
    """Represents a single Booking.com property extracted from search results."""
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
    """A room type / rate offered by a Booking.com property."""
    property_id: str
    room_name: str
    nightly_price: float | None = None
    currency: str = BOOKING_CURRENCY
    max_occupancy: int | None = None
    bed_type: str | None = None


@dataclass
class AvailabilitySnapshot:
    """One night of availability data for a property."""
    property_id: str
    snapshot_date: str  # YYYY-MM-DD
    is_available: bool
    price: float | None = None
    currency: str | None = BOOKING_CURRENCY
    available_rooms: int | None = None


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class BookingScraper:
    """Scrapes Booking.com for Lombok property listings and availability.

    Strategy:
        1. Try the internal GraphQL endpoint (``FullSearch`` operation).
        2. If GraphQL fails (schema change, block, etc.), fall back to
           parsing the search-results HTML page.
        3. Optionally use ``curl_cffi`` for TLS fingerprint spoofing if
           plain ``httpx`` gets blocked (403/captcha).
    """

    def __init__(self, db_path=None):
        self.db_path = db_path or DB_PATH
        self.conn: sqlite3.Connection | None = None
        self.run_id: int | None = None

        # HTTP client -- created fresh per run in ``run()``
        self._client: httpx.Client | None = None
        self._csrf_token: str | None = None

        # Counters for the current run
        self._listings_seen = 0
        self._listings_new = 0
        self._snapshots_added = 0

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _random_ua(self) -> str:
        """Return a random User-Agent string."""
        return random.choice(USER_AGENTS)

    def _base_headers(self) -> dict[str, str]:
        """Headers common to every outbound request."""
        return {
            "User-Agent": self._random_ua(),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "DNT": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        }

    def _build_client(self) -> httpx.Client:
        """Create an httpx client with HTTP/2 support and cookie persistence."""
        return httpx.Client(
            http2=True,
            timeout=httpx.Timeout(REQUEST_TIMEOUT),
            follow_redirects=True,
            headers=self._base_headers(),
        )

    # ------------------------------------------------------------------
    # Step 1: CSRF token
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=2, min=3, max=30),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
        before_sleep=before_sleep_log(logger, 20),  # logging.INFO = 20
        reraise=True,
    )
    def _get_csrf_token(self, checkin: str, checkout: str) -> str:
        """Fetch a search-results page and extract the CSRF token.

        Args:
            checkin: Check-in date string (YYYY-MM-DD).
            checkout: Check-out date string (YYYY-MM-DD).

        Returns:
            The CSRF token string.

        Raises:
            RuntimeError: If the token cannot be extracted.
        """
        params = {
            "ss": "Lombok",
            "checkin": checkin,
            "checkout": checkout,
            "selected_currency": BOOKING_CURRENCY,
        }
        url = f"{SEARCH_URL_BASE}?{urlencode(params)}"
        logger.info("Fetching CSRF token from %s", url)

        resp = self._client.get(url, headers=self._base_headers())
        resp.raise_for_status()

        match = CSRF_PATTERN.search(resp.text)
        if not match:
            logger.warning(
                "CSRF token not found in response (%d chars). "
                "The page structure may have changed.",
                len(resp.text),
            )
            raise RuntimeError("Could not extract CSRF token from Booking.com")

        token = match.group(1)
        logger.info("Obtained CSRF token: %s...", token[:12])
        self._csrf_token = token
        return token

    # ------------------------------------------------------------------
    # Step 2a: GraphQL search (primary)
    # ------------------------------------------------------------------

    def _graphql_search(
        self,
        zone_name: str,
        checkin: str,
        checkout: str,
        offset: int = 0,
        page_size: int = 25,
    ) -> list[dict[str, Any]] | None:
        """Execute a FullSearch GraphQL query and return raw result dicts.

        Returns ``None`` if the GraphQL endpoint is unavailable or the
        response format is unrecognised, signalling the caller to fall
        back to HTML scraping.
        """
        if not self._csrf_token:
            logger.warning("No CSRF token available for GraphQL request.")
            return None

        search_term = f"{zone_name}, Lombok, Indonesia"

        variables = {
            "input": {
                "acidpioneers": True,
                "bookerInputDto": {
                    "selectedCurrency": BOOKING_CURRENCY,
                    "selectedLanguage": BOOKING_LANGUAGE,
                },
                "searchQueryProperties": {
                    "destination": {
                        "searchString": search_term,
                        "destType": "region",
                    },
                    "checkinDate": {
                        "year": int(checkin[:4]),
                        "month": int(checkin[5:7]),
                        "day": int(checkin[8:10]),
                    },
                    "checkoutDate": {
                        "year": int(checkout[:4]),
                        "month": int(checkout[5:7]),
                        "day": int(checkout[8:10]),
                    },
                    "nbAdults": 2,
                    "nbChildren": 0,
                    "nbRooms": 1,
                    "childrenAges": [],
                },
                "pagination": {
                    "offset": offset,
                    "rowsPerPage": page_size,
                },
            },
        }

        # Minimal FullSearch query -- requesting only the fields we need.
        query = """
        query FullSearch($input: SearchQueryInput!) {
          searchQueries {
            search(input: $input) {
              results {
                blocks {
                  finalPrice {
                    amount
                    currency
                  }
                }
                basicPropertyData {
                  id
                  name
                  pageName
                  starRating {
                    value
                  }
                  accommodationTypeId
                  reviewScore {
                    score
                    totalScoreTextTag {
                      translation
                    }
                    reviewCount
                  }
                  location {
                    latitude
                    longitude
                  }
                }
              }
              pagination {
                nbResultsTotal
              }
            }
          }
        }
        """

        headers = {
            "Content-Type": "application/json",
            "Origin": "https://www.booking.com",
            "Referer": f"{SEARCH_URL_BASE}?ss={quote_plus(search_term)}",
            "X-Booking-CSRF-Token": self._csrf_token,
            "User-Agent": self._random_ua(),
        }

        payload = {
            "operationName": "FullSearch",
            "variables": variables,
            "query": query,
        }

        try:
            resp = self._client.post(
                GRAPHQL_URL,
                json=payload,
                headers=headers,
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "GraphQL request returned HTTP %d -- falling back to HTML.",
                exc.response.status_code,
            )
            return None
        except httpx.HTTPError as exc:
            logger.warning("GraphQL request failed (%s) -- falling back to HTML.", exc)
            return None

        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError):
            logger.warning(
                "GraphQL response was not valid JSON (%d bytes) -- "
                "falling back to HTML.",
                len(resp.content),
            )
            return None

        # Navigate the nested response structure
        try:
            search_data = data["data"]["searchQueries"]["search"]
            results = search_data.get("results", [])
            total = (
                search_data.get("pagination", {}).get("nbResultsTotal", 0)
            )
            logger.info(
                "GraphQL returned %d results (total available: %d, offset: %d)",
                len(results),
                total,
                offset,
            )
            return results
        except (KeyError, TypeError) as exc:
            logger.warning(
                "Unexpected GraphQL response structure (%s): %s -- "
                "falling back to HTML.",
                type(exc).__name__,
                str(exc),
            )
            # Log a snippet of the response for debugging
            snippet = json.dumps(data, indent=2)[:500]
            logger.debug("GraphQL response snippet:\n%s", snippet)
            return None

    def _parse_graphql_results(
        self, results: list[dict[str, Any]]
    ) -> list[tuple[BookingProperty, list[RoomType]]]:
        """Convert raw GraphQL result dicts into BookingProperty + RoomType objects."""
        parsed: list[tuple[BookingProperty, list[RoomType]]] = []

        for result in results:
            try:
                basic = result.get("basicPropertyData", {})
                prop_id = str(basic.get("id", ""))
                if not prop_id:
                    continue

                name = basic.get("name", "Unknown")
                page_name = basic.get("pageName", "")
                url = (
                    f"https://www.booking.com/hotel/id/{page_name}.html"
                    if page_name
                    else f"https://www.booking.com/hotel/id/{prop_id}.html"
                )

                star_data = basic.get("starRating") or {}
                star_rating = star_data.get("value")
                if star_rating is not None:
                    star_rating = int(star_rating)

                review_data = basic.get("reviewScore") or {}
                review_score = review_data.get("score")
                if review_score is not None:
                    review_score = float(review_score)
                review_count = review_data.get("reviewCount")
                if review_count is not None:
                    review_count = int(review_count)

                loc = basic.get("location") or {}
                lat = loc.get("latitude")
                lng = loc.get("longitude")
                if lat is not None:
                    lat = float(lat)
                if lng is not None:
                    lng = float(lng)

                zone_id = assign_zone(lat, lng)

                # Accommodation type ID to human-readable (best effort)
                accom_type_id = basic.get("accommodationTypeId")
                property_type = _map_accommodation_type(accom_type_id)

                prop = BookingProperty(
                    property_id=prop_id,
                    name=name,
                    url=url,
                    property_type=property_type,
                    star_rating=star_rating,
                    latitude=lat,
                    longitude=lng,
                    review_score=review_score,
                    review_count=review_count,
                    zone_id=zone_id,
                )

                # Extract room/block pricing
                rooms: list[RoomType] = []
                for block in result.get("blocks", []):
                    price_data = block.get("finalPrice") or {}
                    amount = price_data.get("amount")
                    currency = price_data.get("currency", BOOKING_CURRENCY)
                    if amount is not None:
                        rooms.append(
                            RoomType(
                                property_id=prop_id,
                                room_name="Standard Room",
                                nightly_price=float(amount),
                                currency=currency,
                            )
                        )

                parsed.append((prop, rooms))

            except Exception as exc:
                logger.warning(
                    "Failed to parse GraphQL result entry: %s", exc, exc_info=True
                )
                continue

        return parsed

    # ------------------------------------------------------------------
    # Step 2b: HTML fallback search
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=2, min=3, max=30),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
        before_sleep=before_sleep_log(logger, 20),
        reraise=True,
    )
    def _html_search(
        self,
        zone_name: str,
        checkin: str,
        checkout: str,
        offset: int = 0,
    ) -> tuple[list[tuple[BookingProperty, list[RoomType]]], bool]:
        """Scrape search results HTML as a fallback.

        Returns:
            A tuple of (parsed_results, has_more_pages).
        """
        search_term = f"{zone_name} Lombok Indonesia"
        params = {
            "ss": search_term,
            "dest_type": "region",
            "checkin": checkin,
            "checkout": checkout,
            "selected_currency": BOOKING_CURRENCY,
            "offset": offset,
            "nflt": "",
        }
        url = f"{SEARCH_URL_BASE}?{urlencode(params)}"
        logger.info(
            "HTML fallback search: %s (offset=%d)", zone_name, offset
        )

        resp = self._client.get(url, headers=self._base_headers())

        # Detect captcha / soft-block
        if resp.status_code == 403 or "captcha" in resp.text.lower():
            logger.warning(
                "Received 403/captcha from Booking.com. "
                "Attempting curl_cffi fallback."
            )
            return self._curl_cffi_search(
                url, zone_name, checkin, checkout, offset
            )

        resp.raise_for_status()
        html = resp.text

        return self._parse_html_results(html, zone_name)

    def _curl_cffi_search(
        self,
        url: str,
        zone_name: str,
        checkin: str,
        checkout: str,
        offset: int,
    ) -> tuple[list[tuple[BookingProperty, list[RoomType]]], bool]:
        """Last-resort fallback using curl_cffi for TLS fingerprint spoofing."""
        try:
            from curl_cffi import requests as cffi_requests
        except ImportError:
            logger.error(
                "curl_cffi not installed. Cannot bypass TLS fingerprint check. "
                "Install with: pip install curl-cffi"
            )
            return [], False

        logger.info("Using curl_cffi with Chrome TLS fingerprint for %s", zone_name)

        try:
            resp = cffi_requests.get(
                url,
                impersonate="chrome131",
                headers=self._base_headers(),
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code != 200:
                logger.warning(
                    "curl_cffi also returned HTTP %d for %s",
                    resp.status_code,
                    zone_name,
                )
                return [], False

            return self._parse_html_results(resp.text, zone_name)

        except Exception as exc:
            logger.error("curl_cffi request failed: %s", exc, exc_info=True)
            return [], False

    def _parse_html_results(
        self, html: str, zone_name: str
    ) -> tuple[list[tuple[BookingProperty, list[RoomType]]], bool]:
        """Extract property data from Booking.com search results HTML.

        Returns:
            (list_of_results, has_more_pages)
        """
        parsed: list[tuple[BookingProperty, list[RoomType]]] = []

        # Split HTML into property card chunks for easier extraction.
        # Booking.com wraps each property in a data-testid="property-card" div.
        card_pattern = re.compile(
            r'data-testid="property-card"', re.IGNORECASE
        )
        card_positions = [m.start() for m in card_pattern.finditer(html)]

        if not card_positions:
            logger.warning(
                "No property cards found in HTML for %s (%d chars). "
                "Page structure may have changed.",
                zone_name,
                len(html),
            )
            # Try an alternative card delimiter
            card_positions = [
                m.start()
                for m in re.finditer(r'data-hotelid=', html, re.IGNORECASE)
            ]

        if not card_positions:
            logger.warning(
                "No property entries found at all for %s. Giving up on this page.",
                zone_name,
            )
            return [], False

        logger.info(
            "Found %d property cards in HTML for %s",
            len(card_positions),
            zone_name,
        )

        # Process each card (slice HTML between consecutive positions)
        for i, start in enumerate(card_positions):
            end = (
                card_positions[i + 1] if i + 1 < len(card_positions) else len(html)
            )
            card_html = html[start:end]

            try:
                prop, rooms = self._parse_single_card(card_html)
                if prop:
                    parsed.append((prop, rooms))
            except Exception as exc:
                logger.debug(
                    "Failed to parse property card #%d: %s", i, exc
                )
                continue

        # Check for pagination -- look for "next page" link
        has_more = bool(
            re.search(r'data-testid="pagination-button-next"', html)
            or re.search(r'class="[^"]*bui-pagination__next-arrow', html)
            or re.search(r'class="[^"]*sr_pagination_next', html)
        )

        logger.info(
            "Parsed %d properties from HTML for %s (has_more=%s)",
            len(parsed),
            zone_name,
            has_more,
        )
        return parsed, has_more

    def _parse_single_card(
        self, card_html: str
    ) -> tuple[BookingProperty | None, list[RoomType]]:
        """Extract property data from a single property card HTML snippet."""
        # Property ID
        id_match = PROPERTY_ID_PATTERN.search(card_html)
        if not id_match:
            # Try alternative: sr_item_ prefix
            alt_match = re.search(r'sr_item_(\d+)', card_html)
            if not alt_match:
                return None, []
            prop_id = alt_match.group(1)
        else:
            prop_id = id_match.group(1)

        # Name
        name_match = PROPERTY_NAME_PATTERN.search(card_html)
        if not name_match:
            # Try fallback: aria-label on the title link
            name_match = re.search(
                r'<a[^>]*data-testid="title-link"[^>]*aria-label="([^"]+)"',
                card_html,
            )
        name = name_match.group(1).strip() if name_match else f"Property {prop_id}"

        # URL
        url_match = re.search(
            r'<a[^>]*href="(https?://www\.booking\.com/hotel/[^"]+)"',
            card_html,
        )
        if url_match:
            url = url_match.group(1).split("?")[0]  # strip query params
        else:
            url = f"https://www.booking.com/hotel/id/{prop_id}.html"

        # Star rating
        star_match = re.search(
            r'data-testid="rating-stars"[^>]*>'
            r'((?:<span[^>]*>[^<]*</span>\s*)+)',
            card_html,
            re.DOTALL,
        )
        star_rating = None
        if star_match:
            star_rating = star_match.group(1).count("<span")
        else:
            star_alt = re.search(r'(\d)\s*(?:star|estrella)', card_html, re.I)
            if star_alt:
                star_rating = int(star_alt.group(1))

        # Review score (out of 10)
        review_score = None
        score_match = re.search(
            r'data-testid="review-score"[^>]*>.*?'
            r'([\d]+\.[\d]+|[\d]+)',
            card_html,
            re.DOTALL,
        )
        if score_match:
            try:
                review_score = float(score_match.group(1))
            except ValueError:
                pass

        # Review count
        review_count = None
        count_match = REVIEW_COUNT_PATTERN.search(card_html)
        if count_match:
            try:
                review_count = int(count_match.group(1).replace(",", ""))
            except ValueError:
                pass

        # Coordinates (sometimes embedded in data attributes or JS)
        lat, lng = None, None
        coord_match = re.search(
            r'data-lat=["\']?([-\d.]+)["\']?\s+'
            r'data-lng=["\']?([-\d.]+)["\']?',
            card_html,
        )
        if coord_match:
            try:
                lat = float(coord_match.group(1))
                lng = float(coord_match.group(2))
            except ValueError:
                pass

        # If no coordinates from card, try map-related patterns
        if lat is None:
            map_match = re.search(
                r'"latitude":\s*([-\d.]+)\s*,\s*"longitude":\s*([-\d.]+)',
                card_html,
            )
            if map_match:
                try:
                    lat = float(map_match.group(1))
                    lng = float(map_match.group(2))
                except ValueError:
                    pass

        zone_id = assign_zone(lat, lng)

        # Property type
        prop_type = None
        type_match = re.search(
            r'data-testid="[^"]*property-type[^"]*"[^>]*>([^<]+)<',
            card_html,
        )
        if type_match:
            prop_type = type_match.group(1).strip()

        prop = BookingProperty(
            property_id=prop_id,
            name=name,
            url=url,
            property_type=prop_type,
            star_rating=star_rating,
            latitude=lat,
            longitude=lng,
            review_score=review_score,
            review_count=review_count,
            zone_id=zone_id,
        )

        # Price / room info
        rooms: list[RoomType] = []
        price_match = PRICE_PATTERN.search(card_html)
        if price_match:
            try:
                price_str = price_match.group(1).replace(",", "")
                nightly_price = float(price_str)
                rooms.append(
                    RoomType(
                        property_id=prop_id,
                        room_name="Standard Room",
                        nightly_price=nightly_price,
                        currency=BOOKING_CURRENCY,
                    )
                )
            except ValueError:
                pass

        return prop, rooms

    # ------------------------------------------------------------------
    # Step 3: Search orchestration per zone
    # ------------------------------------------------------------------

    def _search_zone(
        self,
        zone_id: str,
        zone_info: dict,
        checkin: str,
        checkout: str,
    ) -> list[tuple[BookingProperty, list[RoomType]]]:
        """Search a single zone, trying GraphQL first then HTML fallback.

        Handles pagination for both approaches.
        """
        zone_name = zone_info["name"]
        all_results: list[tuple[BookingProperty, list[RoomType]]] = []
        seen_ids: set[str] = set()

        offset = 0
        page = 0
        use_graphql = True

        while page < MAX_PAGES_PER_ZONE:
            page += 1

            if use_graphql:
                raw = self._graphql_search(
                    zone_name, checkin, checkout, offset=offset
                )
                if raw is not None:
                    results = self._parse_graphql_results(raw)
                    for prop, rooms in results:
                        if prop.property_id not in seen_ids:
                            seen_ids.add(prop.property_id)
                            all_results.append((prop, rooms))
                    # GraphQL pagination: if we got a full page, try the next
                    if len(raw) >= 25:
                        offset += 25
                        rate_limit()
                        continue
                    else:
                        break
                else:
                    # GraphQL failed -- switch to HTML for all remaining pages
                    logger.info(
                        "Switching to HTML fallback for zone %s", zone_name
                    )
                    use_graphql = False
                    offset = 0  # HTML uses its own offset scheme

            # HTML fallback path
            try:
                results, has_more = self._html_search(
                    zone_name, checkin, checkout, offset=offset
                )
                for prop, rooms in results:
                    if prop.property_id not in seen_ids:
                        seen_ids.add(prop.property_id)
                        all_results.append((prop, rooms))

                if not has_more or not results:
                    break
                offset += 25
            except Exception as exc:
                logger.error(
                    "HTML search failed for zone %s (page %d): %s",
                    zone_name,
                    page,
                    exc,
                )
                break

            rate_limit()

        logger.info(
            "Zone %s (%s): discovered %d properties",
            zone_id,
            zone_name,
            len(all_results),
        )
        return all_results

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def discover_properties(
        self, checkin: str, checkout: str
    ) -> list[tuple[BookingProperty, list[RoomType]]]:
        """Discover properties across all Lombok zones.

        Args:
            checkin: Check-in date (YYYY-MM-DD).
            checkout: Check-out date (YYYY-MM-DD).

        Returns:
            De-duplicated list of (property, room_types) tuples.
        """
        all_properties: list[tuple[BookingProperty, list[RoomType]]] = []
        global_seen: set[str] = set()

        # Obtain CSRF token before starting zone searches
        try:
            self._get_csrf_token(checkin, checkout)
        except Exception as exc:
            logger.warning(
                "Could not obtain CSRF token (%s). "
                "GraphQL will be unavailable; using HTML fallback only.",
                exc,
            )

        for zone_id, zone_info in ZONES.items():
            logger.info(
                "--- Searching zone: %s (%s) ---", zone_id, zone_info["name"]
            )
            try:
                zone_results = self._search_zone(
                    zone_id, zone_info, checkin, checkout
                )
                for prop, rooms in zone_results:
                    if prop.property_id not in global_seen:
                        global_seen.add(prop.property_id)
                        # Override zone_id if we detected one, otherwise use
                        # the zone we were searching
                        if prop.zone_id is None:
                            prop.zone_id = zone_id
                        all_properties.append((prop, rooms))
            except Exception as exc:
                logger.error(
                    "Failed to search zone %s: %s", zone_id, exc, exc_info=True
                )

            rate_limit()

        logger.info(
            "Discovery complete: %d unique properties found across %d zones",
            len(all_properties),
            len(ZONES),
        )
        return all_properties

    def scrape_availability(
        self,
        properties: list[tuple[BookingProperty, list[RoomType]]],
        days_forward: int = AVAILABILITY_DAYS_FORWARD,
    ) -> list[AvailabilitySnapshot]:
        """Scrape forward availability for each discovered property.

        For each property, we do a quick search with different check-in
        dates to see if the property appears (available) and at what price.
        This is an approximation -- a property not appearing in search
        results for a given date does not necessarily mean it is unavailable,
        but the prices we do capture are reliable.

        Args:
            properties: List of (property, rooms) tuples from discovery.
            days_forward: Number of days into the future to check.

        Returns:
            List of AvailabilitySnapshot records.
        """
        snapshots: list[AvailabilitySnapshot] = []
        today = date.today()

        # Build a set of sample date ranges to check (weekly intervals to
        # keep request volume manageable).
        sample_dates: list[tuple[str, str]] = []
        for day_offset in range(0, days_forward, 7):
            ci = today + timedelta(days=day_offset)
            co = ci + timedelta(days=1)
            sample_dates.append((ci.isoformat(), co.isoformat()))

        # Build a property price index from discovery results
        # (the discovery search already captured one date's pricing)
        prop_map: dict[str, BookingProperty] = {}
        room_map: dict[str, list[RoomType]] = {}
        for prop, rooms in properties:
            prop_map[prop.property_id] = prop
            room_map[prop.property_id] = rooms

        # For properties where we have rooms from the discovery phase,
        # create snapshots using that data as the baseline for the
        # discovery check-in date.
        for prop_id, rooms in room_map.items():
            if rooms:
                best_price = min(
                    (r.nightly_price for r in rooms if r.nightly_price),
                    default=None,
                )
                # The discovery search used a specific checkin; record that
                # as a confirmed availability point.
                snapshots.append(
                    AvailabilitySnapshot(
                        property_id=prop_id,
                        snapshot_date=today.isoformat(),
                        is_available=True,
                        price=best_price,
                        currency=BOOKING_CURRENCY,
                        available_rooms=len(rooms),
                    )
                )

        # Spot-check additional dates by searching each zone for different
        # date windows. This keeps request counts reasonable while building
        # a forward rate curve.
        logger.info(
            "Spot-checking availability across %d date samples for %d properties",
            len(sample_dates),
            len(properties),
        )

        for sample_ci, sample_co in sample_dates[1:]:  # skip today (already captured)
            for zone_id, zone_info in ZONES.items():
                try:
                    results, _ = self._html_search(
                        zone_info["name"], sample_ci, sample_co, offset=0
                    )
                    available_ids: set[str] = set()
                    for prop, rooms in results:
                        available_ids.add(prop.property_id)
                        best_price = None
                        if rooms:
                            best_price = min(
                                (r.nightly_price for r in rooms if r.nightly_price),
                                default=None,
                            )
                        snapshots.append(
                            AvailabilitySnapshot(
                                property_id=prop.property_id,
                                snapshot_date=sample_ci,
                                is_available=True,
                                price=best_price,
                                currency=BOOKING_CURRENCY,
                                available_rooms=len(rooms) if rooms else None,
                            )
                        )
                except Exception as exc:
                    logger.warning(
                        "Availability check failed for zone %s on %s: %s",
                        zone_id,
                        sample_ci,
                        exc,
                    )

                rate_limit()

        logger.info("Collected %d availability snapshots", len(snapshots))
        return snapshots

    # ------------------------------------------------------------------
    # Database persistence
    # ------------------------------------------------------------------

    def _create_scrape_run(self, run_type: str = "full") -> int:
        """Insert a new scrape_runs row and return the run_id."""
        cursor = self.conn.execute(
            """
            INSERT INTO scrape_runs (source, run_type, started_at, status)
            VALUES ('booking', ?, ?, 'running')
            """,
            (run_type, now_iso()),
        )
        self.conn.commit()
        run_id = cursor.lastrowid
        logger.info("Created scrape run #%d (type=%s)", run_id, run_type)
        return run_id

    def _update_scrape_run(self, status: str, error_message: str | None = None):
        """Update the current scrape run with final stats."""
        self.conn.execute(
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
                self.run_id,
            ),
        )
        self.conn.commit()

    def _upsert_property(self, prop: BookingProperty):
        """Insert or update a property in booking_listings."""
        self._listings_seen += 1

        # Check if the property already exists
        existing = self.conn.execute(
            "SELECT property_id FROM booking_listings WHERE property_id = ?",
            (prop.property_id,),
        ).fetchone()

        if existing:
            self.conn.execute(
                """
                UPDATE booking_listings
                SET url = ?,
                    name = ?,
                    property_type = COALESCE(?, property_type),
                    star_rating = COALESCE(?, star_rating),
                    latitude = COALESCE(?, latitude),
                    longitude = COALESCE(?, longitude),
                    zone_id = COALESCE(?, zone_id),
                    review_score = COALESCE(?, review_score),
                    review_count = COALESCE(?, review_count),
                    last_scraped_at = ?,
                    is_active = 1,
                    last_run_id = ?
                WHERE property_id = ?
                """,
                (
                    prop.url,
                    prop.name,
                    prop.property_type,
                    prop.star_rating,
                    prop.latitude,
                    prop.longitude,
                    prop.zone_id,
                    prop.review_score,
                    prop.review_count,
                    now_iso(),
                    self.run_id,
                    prop.property_id,
                ),
            )
        else:
            self._listings_new += 1
            self.conn.execute(
                """
                INSERT INTO booking_listings
                    (property_id, url, name, property_type, star_rating,
                     latitude, longitude, zone_id, review_score, review_count,
                     first_scraped_at, last_scraped_at, is_active, last_run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                """,
                (
                    prop.property_id,
                    prop.url,
                    prop.name,
                    prop.property_type,
                    prop.star_rating,
                    prop.latitude,
                    prop.longitude,
                    prop.zone_id,
                    prop.review_score,
                    prop.review_count,
                    now_iso(),
                    now_iso(),
                    self.run_id,
                ),
            )

    def _upsert_room_types(self, rooms: list[RoomType]):
        """Insert or update room types in booking_room_types."""
        for room in rooms:
            self.conn.execute(
                """
                INSERT INTO booking_room_types
                    (property_id, room_name, max_occupancy, bed_type,
                     is_active, first_seen_at, last_seen_at)
                VALUES (?, ?, ?, ?, 1, ?, ?)
                ON CONFLICT (property_id, room_name) DO UPDATE SET
                    max_occupancy = COALESCE(excluded.max_occupancy, max_occupancy),
                    bed_type = COALESCE(excluded.bed_type, bed_type),
                    is_active = 1,
                    last_seen_at = excluded.last_seen_at
                """,
                (
                    room.property_id,
                    room.room_name,
                    room.max_occupancy,
                    room.bed_type,
                    now_iso(),
                    now_iso(),
                ),
            )

    def _insert_amenities(self, property_id: str, amenities: list[str]):
        """Insert amenities for a property (ignore duplicates)."""
        for amenity in amenities:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO booking_amenities (property_id, amenity)
                VALUES (?, ?)
                """,
                (property_id, amenity),
            )

    def _insert_snapshots(self, snapshots: list[AvailabilitySnapshot]):
        """Bulk-insert calendar snapshots."""
        rows = [
            (
                "booking",
                snap.property_id,
                self.run_id,
                snap.snapshot_date,
                now_iso(),
                1 if snap.is_available else 0,
                snap.price,
                snap.currency,
                snap.available_rooms,
            )
            for snap in snapshots
        ]

        self.conn.executemany(
            """
            INSERT OR IGNORE INTO calendar_snapshots
                (source, listing_id, run_id, snapshot_date, scraped_at,
                 is_available, price, currency, available_rooms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        inserted = self.conn.total_changes
        self._snapshots_added += len(rows)
        logger.info("Inserted %d calendar snapshots", len(rows))

    def _insert_price_history(self, rooms: list[RoomType]):
        """Record current prices in price_history table."""
        for room in rooms:
            if room.nightly_price is None:
                continue

            # Look up room_type_id from booking_room_types
            row = self.conn.execute(
                """
                SELECT room_type_id FROM booking_room_types
                WHERE property_id = ? AND room_name = ?
                """,
                (room.property_id, room.room_name),
            ).fetchone()

            room_type_id = row[0] if row else None

            self.conn.execute(
                """
                INSERT INTO price_history
                    (source, listing_id, room_type_id, run_id,
                     recorded_at, nightly_price, currency)
                VALUES ('booking', ?, ?, ?, ?, ?, ?)
                """,
                (
                    room.property_id,
                    room_type_id,
                    self.run_id,
                    now_iso(),
                    room.nightly_price,
                    room.currency,
                ),
            )

    # ------------------------------------------------------------------
    # Main orchestration
    # ------------------------------------------------------------------

    def run(self, run_type: str = "full") -> dict[str, Any]:
        """Execute a full scrape run.

        Orchestration:
            1. Open database and create scrape_runs record.
            2. Discover properties across all zones.
            3. Persist properties, room types, and amenities.
            4. Scrape forward availability.
            5. Persist calendar snapshots and price history.
            6. Update scrape_runs with final status.

        Args:
            run_type: One of 'full', 'incremental', 'calendar_only'.

        Returns:
            Summary dict with run statistics.
        """
        logger.info("=" * 60)
        logger.info("Starting Booking.com scrape run (type=%s)", run_type)
        logger.info("=" * 60)

        # Set up database connection
        self.conn = get_connection(self.db_path)
        self.run_id = self._create_scrape_run(run_type)
        self._listings_seen = 0
        self._listings_new = 0
        self._snapshots_added = 0

        # Set up HTTP client
        self._client = self._build_client()

        error_message = None
        status = "completed"

        try:
            # -- Discovery --
            today = date.today()
            checkin = (today + timedelta(days=1)).isoformat()
            checkout = (today + timedelta(days=2)).isoformat()

            properties = self.discover_properties(checkin, checkout)

            if not properties:
                logger.warning(
                    "No properties discovered. Booking.com may be blocking requests."
                )
                status = "partial"

            # -- Persist listings --
            logger.info("Persisting %d properties to database...", len(properties))
            for prop, rooms in properties:
                try:
                    self._upsert_property(prop)
                    if rooms:
                        self._upsert_room_types(rooms)
                        self._insert_price_history(rooms)
                except Exception as exc:
                    logger.error(
                        "Failed to persist property %s: %s",
                        prop.property_id,
                        exc,
                    )

            self.conn.commit()

            # -- Availability (skip for calendar_only if no properties) --
            if run_type != "calendar_only" or properties:
                logger.info("Scraping forward availability...")
                snapshots = self.scrape_availability(properties)

                if snapshots:
                    self._insert_snapshots(snapshots)
                    self.conn.commit()
                else:
                    logger.warning("No availability snapshots collected.")

        except KeyboardInterrupt:
            logger.warning("Scrape interrupted by user.")
            status = "partial"
            error_message = "Interrupted by user"
        except Exception as exc:
            logger.error("Scrape run failed: %s", exc, exc_info=True)
            status = "failed"
            error_message = str(exc)[:500]
        finally:
            # Update run record
            self._update_scrape_run(status, error_message)

            # Clean up
            if self._client:
                self._client.close()
                self._client = None
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
        logger.info(
            "  Listings seen: %d | New: %d | Snapshots: %d",
            self._listings_seen,
            self._listings_new,
            self._snapshots_added,
        )
        logger.info("=" * 60)

        return summary


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Booking.com accommodation type ID to human-readable mapping (partial).
# These IDs come from Booking's internal classification system.
_ACCOMMODATION_TYPE_MAP = {
    1: "hotel",
    2: "apartment",
    3: "hostel",
    4: "guesthouse",
    5: "bed_and_breakfast",
    7: "resort",
    8: "motel",
    9: "inn",
    11: "villa",
    12: "holiday_home",
    13: "camping",
    14: "homestay",
    15: "lodge",
    17: "country_house",
    19: "farm_stay",
    20: "boat",
    21: "luxury_tent",
    22: "capsule_hotel",
    23: "love_hotel",
    24: "ryokan",
    25: "holiday_park",
    27: "resort_village",
    28: "chalet",
    29: "cottage",
    31: "condo_hotel",
    32: "riad",
    33: "aparthotel",
}


def _map_accommodation_type(type_id: int | None) -> str | None:
    """Map a Booking.com accommodation type ID to a readable string."""
    if type_id is None:
        return None
    return _ACCOMMODATION_TYPE_MAP.get(type_id, f"type_{type_id}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Scrape Booking.com listings and availability for Lombok."
    )
    parser.add_argument(
        "--run-type",
        choices=["full", "incremental", "calendar_only"],
        default="full",
        help="Type of scrape run (default: full).",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Override database path (default: use config.DB_PATH).",
    )
    args = parser.parse_args()

    from pathlib import Path

    db_path = Path(args.db_path) if args.db_path else None
    scraper = BookingScraper(db_path=db_path)
    result = scraper.run(run_type=args.run_type)

    print(f"\nRun #{result['run_id']} finished with status: {result['status']}")
    print(f"  Listings seen: {result['listings_seen']}")
    print(f"  Listings new:  {result['listings_new']}")
    print(f"  Snapshots:     {result['snapshots_added']}")
    if result["error_message"]:
        print(f"  Error: {result['error_message']}")
