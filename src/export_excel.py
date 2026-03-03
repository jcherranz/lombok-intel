"""Export the SQLite database to a multi-sheet Excel file after each run."""

import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.db.init_db import DB_PATH
from src.utils import setup_logger

logger = setup_logger("export_excel")

EXPORT_PATH = Path(__file__).resolve().parent.parent / "data" / "lombok_intel.xlsx"


def export(db_path: str = DB_PATH, out_path: Path = EXPORT_PATH):
    """Dump key tables and views into a single .xlsx with one sheet each."""
    conn = sqlite3.connect(db_path)
    try:
        return _export_inner(conn, out_path)
    finally:
        conn.close()


def _export_inner(conn, out_path: Path) -> Path:
    sheets = {
        "Listings": """
            SELECT listing_id, name, property_type, zone_id, latitude, longitude,
                   nightly_price AS price_usd, rating_overall, review_count,
                   accommodates, bedrooms, beds, bathrooms,
                   is_superhost, instant_bookable, first_scraped_at, last_scraped_at
            FROM airbnb_listings
            WHERE is_active = 1
            ORDER BY zone_id, nightly_price DESC
        """,
        "Booking Listings": """
            SELECT property_id, name, property_type, zone_id, latitude, longitude,
                   star_rating, review_score, review_count,
                   first_scraped_at, last_scraped_at
            FROM booking_listings
            WHERE is_active = 1
            ORDER BY zone_id, review_score DESC
        """,
        "Zone Summary": """
            SELECT z.zone_id, z.name AS zone_name,
                   COUNT(*) AS listings,
                   ROUND(AVG(a.nightly_price), 1) AS avg_price,
                   ROUND(MIN(a.nightly_price), 1) AS min_price,
                   ROUND(MAX(a.nightly_price), 1) AS max_price,
                   ROUND(AVG(a.rating_overall), 2) AS avg_rating,
                   SUM(a.review_count) AS total_reviews
            FROM airbnb_listings a
            JOIN zones z ON a.zone_id = z.zone_id
            WHERE a.is_active = 1
            GROUP BY z.zone_id
            ORDER BY listings DESC
        """,
        "Combined Zone Summary": """
            SELECT z.zone_id, z.name AS zone_name,
                   SUM(CASE WHEN source = 'airbnb' THEN 1 ELSE 0 END) AS airbnb_listings,
                   SUM(CASE WHEN source = 'booking' THEN 1 ELSE 0 END) AS booking_listings,
                   COUNT(*) AS total_listings,
                   ROUND(AVG(nightly_price), 1) AS avg_price
            FROM (
                SELECT zone_id, 'airbnb' AS source, nightly_price
                FROM airbnb_listings WHERE is_active = 1
                UNION ALL
                SELECT zone_id, 'booking' AS source, NULL AS nightly_price
                FROM booking_listings WHERE is_active = 1
            ) combined
            JOIN zones z ON combined.zone_id = z.zone_id
            GROUP BY z.zone_id
            ORDER BY total_listings DESC
        """,
        "Calendar": """
            SELECT cs.listing_id, a.name, a.zone_id, cs.snapshot_date,
                   cs.is_available, cs.price, a.nightly_price AS base_price
            FROM calendar_snapshots cs
            JOIN airbnb_listings a ON cs.listing_id = a.listing_id
            WHERE cs.source = 'airbnb'
            ORDER BY a.zone_id, cs.listing_id, cs.snapshot_date
        """,
        "Booking Calendar": """
            SELECT cs.listing_id AS property_id, b.name, b.zone_id,
                   cs.snapshot_date, cs.is_available, cs.price
            FROM calendar_snapshots cs
            JOIN booking_listings b ON cs.listing_id = b.property_id
            WHERE cs.source = 'booking'
            ORDER BY b.zone_id, cs.listing_id, cs.snapshot_date
        """,
        "ADR by Zone": """
            SELECT * FROM v_adr_simple
        """,
        "Occupancy": """
            SELECT * FROM v_occupancy_monthly
        """,
        "Supply Growth": """
            SELECT * FROM v_supply_growth
        """,
        "Scrape Health": """
            SELECT * FROM v_scrape_health
        """,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)

    failed_sheets = []
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for sheet_name, query in sheets.items():
            try:
                df = pd.read_sql_query(query, conn)
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                logger.info("  %s: %d rows", sheet_name, len(df))
            except Exception as e:
                logger.warning("  %s: skipped (%s)", sheet_name, e)
                failed_sheets.append(sheet_name)

    if failed_sheets:
        logger.error("Excel export incomplete — failed sheets: %s", ", ".join(failed_sheets))
        raise RuntimeError(f"Excel export incomplete: {', '.join(failed_sheets)} failed")

    logger.info("Excel exported → %s", out_path)
    return out_path


if __name__ == "__main__":
    export()
