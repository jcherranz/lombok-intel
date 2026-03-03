"""Tests for hardening fixes: ADR dedup, migration idempotency, pipeline exit codes."""

import sqlite3
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.db.init_db import init_database, get_connection, _apply_migrations, _migrations_applied


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_test_db(tmp_path: Path) -> tuple[sqlite3.Connection, Path]:
    """Create a test database with schema applied. Returns (conn, db_path)."""
    db_path = tmp_path / "test.db"
    conn = init_database(db_path)
    conn.row_factory = sqlite3.Row
    return conn, db_path


def _seed_zone(conn: sqlite3.Connection, zone_id: str = "KUT"):
    """Insert a zone so foreign-key-like lookups work."""
    conn.execute(
        "INSERT OR IGNORE INTO zones (zone_id, name, zone_priority, lat_min, lat_max, lng_min, lng_max) "
        "VALUES (?, 'Test Zone', 50, -9.0, -8.0, 116.0, 117.0)",
        (zone_id,),
    )


# ---------------------------------------------------------------------------
# 1. ADR dedup CTE: latest run wins, no double-counting
# ---------------------------------------------------------------------------

def test_adr_dedup_latest_run_wins(tmp_path):
    """ADR computation should use only the latest run per (source, listing, date)."""
    conn, db_path = _make_test_db(tmp_path)
    _seed_zone(conn)

    # Create two scrape runs
    conn.execute(
        "INSERT INTO scrape_runs (source, run_type, started_at, status) "
        "VALUES ('airbnb', 'full', '2026-03-01 00:00:00', 'completed')"
    )
    conn.execute(
        "INSERT INTO scrape_runs (source, run_type, started_at, status) "
        "VALUES ('airbnb', 'full', '2026-03-02 00:00:00', 'completed')"
    )

    # Create a listing
    conn.execute(
        "INSERT INTO airbnb_listings "
        "(listing_id, url, name, zone_id, first_scraped_at, last_scraped_at, is_active, last_run_id) "
        "VALUES ('L1', 'http://test', 'Test', 'KUT', '2026-03-01', '2026-03-02', 1, 2)"
    )

    # Run 1: price = $100 for same date
    conn.execute(
        "INSERT INTO calendar_snapshots "
        "(source, listing_id, run_id, snapshot_date, scraped_at, is_available, price, currency) "
        "VALUES ('airbnb', 'L1', 1, '2026-03-10', '2026-03-01', 1, 100.0, 'USD')"
    )
    # Run 2: price = $200 for same date (later run should win)
    conn.execute(
        "INSERT INTO calendar_snapshots "
        "(source, listing_id, run_id, snapshot_date, scraped_at, is_available, price, currency) "
        "VALUES ('airbnb', 'L1', 2, '2026-03-10', '2026-03-02', 1, 200.0, 'USD')"
    )
    conn.commit()
    conn.close()

    # Run ADR calculator
    from src.pipeline.adr_calculator import ADRCalculator
    calc = ADRCalculator(db_path=db_path)
    df = calc.compute_zone_adr(zone_id="KUT")

    assert len(df) == 1
    # Should be $200 (run 2), not $150 (average of both) or $100 (run 1)
    assert df.iloc[0]["adr"] == 200.0
    assert df.iloc[0]["sample_size"] == 1  # Deduped to 1 snapshot, not 2


def test_adr_dedup_multi_source_no_collision(tmp_path):
    """Airbnb and Booking listings with the same ID don't collide in ADR."""
    conn, db_path = _make_test_db(tmp_path)
    _seed_zone(conn)

    conn.execute(
        "INSERT INTO scrape_runs (source, run_type, started_at, status) "
        "VALUES ('airbnb', 'full', '2026-03-01 00:00:00', 'completed')"
    )
    conn.execute(
        "INSERT INTO scrape_runs (source, run_type, started_at, status) "
        "VALUES ('booking', 'full', '2026-03-01 00:00:00', 'completed')"
    )

    # Same ID "X1" in both sources
    conn.execute(
        "INSERT INTO airbnb_listings "
        "(listing_id, url, name, zone_id, first_scraped_at, last_scraped_at, is_active, last_run_id) "
        "VALUES ('X1', 'http://test', 'Airbnb X1', 'KUT', '2026-03-01', '2026-03-01', 1, 1)"
    )
    conn.execute(
        "INSERT INTO booking_listings "
        "(property_id, url, name, zone_id, first_scraped_at, last_scraped_at, is_active, last_run_id) "
        "VALUES ('X1', 'http://test', 'Booking X1', 'KUT', '2026-03-01', '2026-03-01', 1, 2)"
    )

    # Airbnb snapshot: $100
    conn.execute(
        "INSERT INTO calendar_snapshots "
        "(source, listing_id, run_id, snapshot_date, scraped_at, is_available, price, currency) "
        "VALUES ('airbnb', 'X1', 1, '2026-03-10', '2026-03-01', 1, 100.0, 'USD')"
    )
    # Booking snapshot: $300
    conn.execute(
        "INSERT INTO calendar_snapshots "
        "(source, listing_id, run_id, snapshot_date, scraped_at, is_available, price, currency) "
        "VALUES ('booking', 'X1', 2, '2026-03-10', '2026-03-01', 1, 300.0, 'USD')"
    )
    conn.commit()
    conn.close()

    from src.pipeline.adr_calculator import ADRCalculator
    calc = ADRCalculator(db_path=db_path)
    df = calc.compute_zone_adr(zone_id="KUT")

    assert len(df) == 1
    # Both snapshots should count (different sources)
    assert df.iloc[0]["sample_size"] == 2
    assert df.iloc[0]["adr"] == 200.0  # avg(100, 300)


# ---------------------------------------------------------------------------
# 2. Migration idempotency
# ---------------------------------------------------------------------------

def test_migration_idempotent(tmp_path):
    """Running _apply_migrations multiple times doesn't error or duplicate data."""
    conn, db_path = _make_test_db(tmp_path)

    # Insert a scrape run (needed for FK)
    conn.execute(
        "INSERT INTO scrape_runs (source, run_type, started_at, status) "
        "VALUES ('airbnb', 'full', '2026-03-01 00:00:00', 'completed')"
    )

    # Insert occupancy events (some duplicates)
    for i in range(3):
        conn.execute(
            "INSERT OR IGNORE INTO occupancy_events "
            "(source, listing_id, zone_id, event_date, event_type, transition, "
            " prev_run_id, curr_run_id, detected_at) "
            "VALUES ('airbnb', 'L1', 'KUT', '2026-03-05', 'probable_booking', "
            " 'available_to_blocked', 1, 1, '2026-03-01')"
        )
    conn.commit()

    # Count before
    count_before = conn.execute("SELECT COUNT(*) FROM occupancy_events").fetchone()[0]

    # Run migrations again (should be idempotent)
    _apply_migrations(conn)
    conn.commit()

    count_after = conn.execute("SELECT COUNT(*) FROM occupancy_events").fetchone()[0]
    assert count_after == count_before  # No rows lost or added
    assert count_after == 1  # INSERT OR IGNORE deduped the 3 inserts to 1


def test_get_connection_migrations_once_per_process(tmp_path):
    """get_connection applies migrations only once per DB path per process."""
    db_path = tmp_path / "test.db"
    init_database(db_path)

    # Clear the tracking set so we can test fresh
    db_key = str(db_path)
    _migrations_applied.discard(db_key)

    conn1 = get_connection(db_path)
    assert db_key in _migrations_applied

    # Patch _apply_migrations to track calls
    with patch("src.db.init_db._apply_migrations") as mock_migrate:
        conn2 = get_connection(db_path)
        # Should NOT call _apply_migrations again for same path
        mock_migrate.assert_not_called()

    conn1.close()
    conn2.close()

    # Clean up for other tests
    _migrations_applied.discard(db_key)


# ---------------------------------------------------------------------------
# 3. Occupancy rate capped at 1.0
# ---------------------------------------------------------------------------

def test_occupancy_rate_capped_at_one(tmp_path):
    """Occupancy rate should never exceed 1.0 even if booked > total nights."""
    conn, db_path = _make_test_db(tmp_path)
    _seed_zone(conn)

    # Two scrape runs
    conn.execute(
        "INSERT INTO scrape_runs (source, run_type, started_at, status) "
        "VALUES ('airbnb', 'full', '2026-03-01 00:00:00', 'completed')"
    )
    conn.execute(
        "INSERT INTO scrape_runs (source, run_type, started_at, status) "
        "VALUES ('airbnb', 'full', '2026-03-02 00:00:00', 'completed')"
    )

    # Listing
    conn.execute(
        "INSERT INTO airbnb_listings "
        "(listing_id, url, name, zone_id, first_scraped_at, last_scraped_at, is_active, last_run_id) "
        "VALUES ('L1', 'http://test', 'Test', 'KUT', '2026-03-01', '2026-03-02', 1, 2)"
    )

    # 2 calendar snapshots — 1 listing, 2 dates (this gives total_nights = 2)
    for snap_date in ["2026-03-10", "2026-03-11"]:
        conn.execute(
            "INSERT INTO calendar_snapshots "
            "(source, listing_id, run_id, snapshot_date, scraped_at, is_available, price, currency) "
            "VALUES ('airbnb', 'L1', 2, ?, '2026-03-02', 1, 100.0, 'USD')",
            (snap_date,),
        )

    # 3 occupancy events for same listing (more bookings than total nights)
    for day in ["10", "11", "12"]:
        conn.execute(
            "INSERT OR IGNORE INTO occupancy_events "
            "(source, listing_id, zone_id, event_date, event_type, transition, "
            " prev_run_id, curr_run_id, detected_at) "
            "VALUES ('airbnb', 'L1', 'KUT', ?, 'probable_booking', "
            " 'available_to_blocked', 1, 2, '2026-03-02')",
            (f"2026-03-{day}",),
        )
    conn.commit()
    conn.close()

    from src.pipeline.adr_calculator import ADRCalculator
    calc = ADRCalculator(db_path=db_path)
    df = calc.compute_zone_occupancy(zone_id="KUT")

    assert not df.empty, "Expected occupancy data but got empty DataFrame"
    assert df["occupancy_rate"].max() <= 1.0
