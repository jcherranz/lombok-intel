"""Tests for src/export_excel.py — Excel output has expected sheets."""

import sqlite3
from pathlib import Path

import pandas as pd

from src.db.init_db import init_database
from src.export_excel import export


def _seed_test_db(db_path: Path):
    """Create and seed a minimal test database."""
    conn = init_database(db_path)

    # Insert a scrape run
    conn.execute(
        "INSERT INTO scrape_runs (source, run_type, started_at, status) "
        "VALUES ('airbnb', 'full', '2026-03-01 00:00:00', 'completed')"
    )

    # Insert a listing
    conn.execute(
        "INSERT INTO airbnb_listings "
        "(listing_id, url, name, zone_id, latitude, longitude, nightly_price, "
        " rating_overall, review_count, accommodates, bedrooms, "
        " first_scraped_at, last_scraped_at, is_active, last_run_id) "
        "VALUES ('999', 'https://airbnb.com/rooms/999', 'Test Villa', 'KUT', -8.85, 116.30, 150.0, "
        " 4.8, 50, 6, 3, '2026-03-01', '2026-03-01', 1, 1)"
    )

    # Insert a calendar snapshot
    conn.execute(
        "INSERT INTO calendar_snapshots "
        "(source, listing_id, run_id, snapshot_date, scraped_at, is_available, price, currency) "
        "VALUES ('airbnb', '999', 1, '2026-03-15', '2026-03-01', 1, 150.0, 'USD')"
    )
    conn.commit()
    conn.close()


def test_export_creates_file(tmp_path):
    """Export creates an xlsx file with expected sheets."""
    db_path = tmp_path / "test.db"
    xlsx_path = tmp_path / "test.xlsx"
    _seed_test_db(db_path)

    result = export(db_path=str(db_path), out_path=xlsx_path)
    assert result == xlsx_path
    assert xlsx_path.exists()

    # Check sheet names
    xl = pd.ExcelFile(xlsx_path)
    expected = {"Listings", "Zone Summary", "Calendar", "ADR by Zone", "Scrape Health"}
    assert expected.issubset(set(xl.sheet_names))


def test_export_listings_sheet_has_data(tmp_path):
    """The Listings sheet should contain the seeded listing."""
    db_path = tmp_path / "test.db"
    xlsx_path = tmp_path / "test.xlsx"
    _seed_test_db(db_path)
    export(db_path=str(db_path), out_path=xlsx_path)

    df = pd.read_excel(xlsx_path, sheet_name="Listings")
    assert len(df) >= 1
    assert "Test Villa" in df["name"].values
