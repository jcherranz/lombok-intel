"""Tests for occupancy engine classification logic."""

import sqlite3
from pathlib import Path

from src.db.init_db import init_database


def _make_test_db(tmp_path: Path) -> sqlite3.Connection:
    """Create an in-memory test database with schema applied."""
    db_path = tmp_path / "test.db"
    conn = init_database(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def test_calendar_snapshot_available_counts(tmp_path):
    """Inserting available/blocked snapshots gives correct counts."""
    conn = _make_test_db(tmp_path)

    # Insert a scrape run
    conn.execute(
        "INSERT INTO scrape_runs (source, run_type, started_at, status) "
        "VALUES ('airbnb', 'full', '2026-03-01 00:00:00', 'completed')"
    )
    # Insert a listing
    conn.execute(
        "INSERT INTO airbnb_listings (listing_id, url, name, first_scraped_at, last_scraped_at, is_active, last_run_id) "
        "VALUES ('123', 'https://airbnb.com/rooms/123', 'Test', '2026-03-01', '2026-03-01', 1, 1)"
    )
    # Insert snapshots: 5 available, 3 blocked
    for day in range(1, 9):
        conn.execute(
            "INSERT INTO calendar_snapshots "
            "(source, listing_id, run_id, snapshot_date, scraped_at, is_available, price, currency) "
            "VALUES ('airbnb', '123', 1, ?, '2026-03-01', ?, 100.0, 'USD')",
            (f"2026-03-{day:02d}", 1 if day <= 5 else 0),
        )
    conn.commit()

    avail = conn.execute(
        "SELECT COUNT(*) FROM calendar_snapshots WHERE is_available = 1"
    ).fetchone()[0]
    blocked = conn.execute(
        "SELECT COUNT(*) FROM calendar_snapshots WHERE is_available = 0"
    ).fetchone()[0]

    assert avail == 5
    assert blocked == 3


def test_occupancy_event_classification(tmp_path):
    """Occupancy events can be inserted with correct event types."""
    conn = _make_test_db(tmp_path)

    conn.execute(
        "INSERT INTO scrape_runs (source, run_type, started_at, status) "
        "VALUES ('airbnb', 'full', '2026-03-01 00:00:00', 'completed')"
    )

    conn.execute(
        "INSERT INTO occupancy_events "
        "(source, listing_id, zone_id, event_date, event_type, transition, "
        " prev_run_id, curr_run_id, detected_at) "
        "VALUES ('airbnb', '123', 'KUT', '2026-03-05', 'probable_booking', "
        " 'available_to_blocked', 1, 1, '2026-03-01')"
    )
    conn.commit()

    row = conn.execute(
        "SELECT event_type FROM occupancy_events WHERE listing_id = '123'"
    ).fetchone()
    assert row["event_type"] == "probable_booking"


def test_owner_block_vs_booking(tmp_path):
    """Both event types can coexist in the table."""
    conn = _make_test_db(tmp_path)

    conn.execute(
        "INSERT INTO scrape_runs (source, run_type, started_at, status) "
        "VALUES ('airbnb', 'full', '2026-03-01 00:00:00', 'completed')"
    )

    for event_type, day in [("probable_booking", "05"), ("owner_block", "06")]:
        conn.execute(
            "INSERT INTO occupancy_events "
            "(source, listing_id, zone_id, event_date, event_type, transition, "
            " prev_run_id, curr_run_id, detected_at) "
            "VALUES ('airbnb', '123', 'KUT', ?, ?, "
            " 'available_to_blocked', 1, 1, '2026-03-01')",
            (f"2026-03-{day}", event_type),
        )
    conn.commit()

    bookings = conn.execute(
        "SELECT COUNT(*) FROM occupancy_events WHERE event_type = 'probable_booking'"
    ).fetchone()[0]
    blocks = conn.execute(
        "SELECT COUNT(*) FROM occupancy_events WHERE event_type = 'owner_block'"
    ).fetchone()[0]
    assert bookings == 1
    assert blocks == 1
