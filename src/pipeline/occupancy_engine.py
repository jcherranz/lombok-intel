"""Occupancy inference engine for Lombok Market Intelligence.

Compares consecutive calendar snapshot runs to detect booking transitions.
For Airbnb listings, dates that flip from available to blocked are classified
as probable bookings, owner blocks, or unknown. For Booking.com listings,
room-count decreases between snapshots are used to compute occupancy.

Typical usage:
    engine = OccupancyEngine()
    engine.run()                 # process latest unprocessed run
    engine.run(run_id=42)        # process a specific run
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd

from src.config import DB_PATH, CALENDAR_DAYS_FORWARD
from src.utils import setup_logger, now_iso
from src.db.init_db import get_connection

logger = setup_logger("occupancy_engine")

# ---------------------------------------------------------------------------
# Heuristic thresholds
# ---------------------------------------------------------------------------
MAX_REALISTIC_BOOKING_WINDOW_DAYS = 90
OWNER_BLOCK_MIN_CONSECUTIVE_DAYS = 30


class OccupancyEngine:
    """Infer occupancy events from calendar snapshot diffs."""

    def __init__(self, db_path=None):
        self.db_path = db_path or DB_PATH
        logger.info("OccupancyEngine initialised (db=%s)", self.db_path)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_conn(self) -> sqlite3.Connection:
        """Open a connection with row_factory enabled."""
        return get_connection(self.db_path)

    def _get_recent_run_pair(
        self, conn: sqlite3.Connection, source: str
    ) -> tuple[Optional[int], Optional[int]]:
        """Return (prev_run_id, curr_run_id) for the two most recent
        completed runs of the given source.  Returns (None, None) when
        fewer than two completed runs exist."""
        rows = conn.execute(
            """
            SELECT run_id
            FROM scrape_runs
            WHERE source = ? AND status IN ('completed', 'partial')
            ORDER BY started_at DESC
            LIMIT 2
            """,
            (source,),
        ).fetchall()

        if len(rows) < 2:
            return (None, None)
        # rows[0] is the newest, rows[1] is the second-newest
        return (rows[1]["run_id"], rows[0]["run_id"])

    def _get_run_pair_for_id(
        self, conn: sqlite3.Connection, run_id: int, source: str
    ) -> tuple[Optional[int], int]:
        """Given an explicit curr_run_id, find the previous completed run
        for the same source.  Returns (prev_run_id, curr_run_id)."""
        row = conn.execute(
            """
            SELECT run_id
            FROM scrape_runs
            WHERE source = ? AND status IN ('completed', 'partial')
              AND run_id < ?
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (source, run_id),
        ).fetchone()

        prev_run_id = row["run_id"] if row else None
        return (prev_run_id, run_id)

    def _lookup_zone(
        self, conn: sqlite3.Connection, source: str, listing_id: str
    ) -> Optional[str]:
        """Return the zone_id for a listing from its master table."""
        if source == "airbnb":
            row = conn.execute(
                "SELECT zone_id FROM airbnb_listings WHERE listing_id = ?",
                (listing_id,),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT zone_id FROM booking_listings WHERE property_id = ?",
                (listing_id,),
            ).fetchone()
        return row["zone_id"] if row else None

    # ------------------------------------------------------------------
    # Event classification
    # ------------------------------------------------------------------

    @staticmethod
    def classify_event(
        listing_id: str,
        event_date: str,
        transition: str,
        consecutive_blocked_days: int,
        minimum_nights: Optional[int],
    ) -> str:
        """Classify a calendar transition into an event type.

        Args:
            listing_id: The listing this event belongs to.
            event_date: ISO date string of the night that changed state.
            transition: 'available_to_blocked' or 'blocked_to_available'.
            consecutive_blocked_days: How many consecutive days are blocked
                around this date in the current snapshot.
            minimum_nights: The listing's minimum_nights setting (may be None).

        Returns:
            One of 'probable_booking', 'owner_block', or 'unknown'.
        """
        if transition == "blocked_to_available":
            # A cancellation or owner unblocking -- not a booking.
            return "unknown"

        # transition == "available_to_blocked" from here on
        # ----------------------------------------------------------
        # Owner block: unusually long blocked stretch with no
        # apparent guest stay pattern.
        if consecutive_blocked_days > OWNER_BLOCK_MIN_CONSECUTIVE_DAYS:
            return "owner_block"

        # Booking window check: the event_date should be within a
        # realistic booking horizon from today.
        try:
            event_dt = datetime.strptime(event_date, "%Y-%m-%d").date()
            days_out = (event_dt - date.today()).days
            if days_out > MAX_REALISTIC_BOOKING_WINDOW_DAYS:
                return "owner_block"
        except ValueError:
            pass

        # Minimum nights compatibility: if the blocked stretch is
        # shorter than the listing's minimum_nights it is unlikely
        # to be a genuine booking.
        if minimum_nights and consecutive_blocked_days < minimum_nights:
            return "unknown"

        return "probable_booking"

    # ------------------------------------------------------------------
    # Airbnb transitions
    # ------------------------------------------------------------------

    def process_airbnb_transitions(self, run_id: Optional[int] = None) -> int:
        """Compare the latest (or specified) Airbnb run with its predecessor.

        Identifies dates where ``is_available`` flipped between the two
        snapshots and inserts classified ``occupancy_events`` rows.

        Returns:
            Number of occupancy events inserted.
        """
        conn = self._get_conn()
        try:
            if run_id is not None:
                prev_run, curr_run = self._get_run_pair_for_id(
                    conn, run_id, "airbnb"
                )
            else:
                prev_run, curr_run = self._get_recent_run_pair(conn, "airbnb")

            if prev_run is None or curr_run is None:
                logger.warning(
                    "Not enough Airbnb runs to compare (prev=%s, curr=%s)",
                    prev_run,
                    curr_run,
                )
                return 0

            logger.info(
                "Comparing Airbnb runs: prev=%d, curr=%d", prev_run, curr_run
            )

            # Pull both snapshots into pandas for a clean merge.
            query = """
                SELECT listing_id, snapshot_date, is_available, price, currency
                FROM calendar_snapshots
                WHERE source = 'airbnb' AND run_id = ?
            """
            df_prev = pd.read_sql_query(query, conn, params=(prev_run,))
            df_curr = pd.read_sql_query(query, conn, params=(curr_run,))

            if df_prev.empty or df_curr.empty:
                logger.warning(
                    "One or both snapshot sets are empty "
                    "(prev rows=%d, curr rows=%d)",
                    len(df_prev),
                    len(df_curr),
                )
                return 0

            # Merge on (listing_id, snapshot_date)
            merged = df_prev.merge(
                df_curr,
                on=["listing_id", "snapshot_date"],
                suffixes=("_prev", "_curr"),
                how="inner",
            )

            # Keep only rows where availability changed
            transitions = merged[
                merged["is_available_prev"] != merged["is_available_curr"]
            ].copy()

            if transitions.empty:
                logger.info("No availability transitions detected.")
                return 0

            logger.info(
                "Found %d date-level transitions across %d listings",
                len(transitions),
                transitions["listing_id"].nunique(),
            )

            # Classify transitions: determine direction
            transitions["transition"] = transitions.apply(
                lambda r: (
                    "available_to_blocked"
                    if r["is_available_prev"] == 1
                    and r["is_available_curr"] == 0
                    else "blocked_to_available"
                ),
                axis=1,
            )

            # Compute consecutive blocked days per listing in the
            # current snapshot (needed for classification heuristics).
            blocked_curr = df_curr[df_curr["is_available"] == 0].copy()
            blocked_streaks = self._compute_blocked_streaks(blocked_curr)

            # Fetch minimum_nights for each listing
            listing_ids = transitions["listing_id"].unique().tolist()
            min_nights_map = self._fetch_min_nights(conn, listing_ids)

            # Classify and prepare insert rows
            detected_at = now_iso()
            events = []
            for _, row in transitions.iterrows():
                lid = row["listing_id"]
                sdate = row["snapshot_date"]
                transition = row["transition"]

                consec = blocked_streaks.get((lid, sdate), 1)
                min_n = min_nights_map.get(lid)

                event_type = self.classify_event(
                    lid, sdate, transition, consec, min_n
                )
                zone_id = self._lookup_zone(conn, "airbnb", lid)

                last_price = (
                    row["price_prev"]
                    if pd.notna(row.get("price_prev"))
                    else None
                )
                currency = (
                    row["currency_prev"]
                    if pd.notna(row.get("currency_prev"))
                    else None
                )

                events.append(
                    (
                        "airbnb",
                        lid,
                        zone_id,
                        sdate,
                        detected_at,
                        prev_run,
                        curr_run,
                        transition,
                        event_type,
                        last_price,
                        currency,
                    )
                )

            if events:
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO occupancy_events
                        (source, listing_id, zone_id, event_date, detected_at,
                         prev_run_id, curr_run_id, transition, event_type,
                         last_known_price, currency)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    events,
                )
                conn.commit()

            logger.info("Inserted %d Airbnb occupancy events.", len(events))
            return len(events)

        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Booking.com transitions
    # ------------------------------------------------------------------

    def process_booking_occupancy(self, run_id: Optional[int] = None) -> int:
        """Compare the latest (or specified) Booking.com run with its
        predecessor using ``available_rooms`` counts.

        When ``available_rooms`` decreases between snapshots, the difference
        is treated as rooms booked. An ``available_to_blocked`` event is
        inserted for each such date. When rooms free up, a
        ``blocked_to_available`` event is inserted.

        Returns:
            Number of occupancy events inserted.
        """
        conn = self._get_conn()
        try:
            if run_id is not None:
                prev_run, curr_run = self._get_run_pair_for_id(
                    conn, run_id, "booking"
                )
            else:
                prev_run, curr_run = self._get_recent_run_pair(conn, "booking")

            if prev_run is None or curr_run is None:
                logger.warning(
                    "Not enough Booking.com runs to compare (prev=%s, curr=%s)",
                    prev_run,
                    curr_run,
                )
                return 0

            logger.info(
                "Comparing Booking.com runs: prev=%d, curr=%d",
                prev_run,
                curr_run,
            )

            query = """
                SELECT listing_id, snapshot_date, is_available,
                       available_rooms, price, currency
                FROM calendar_snapshots
                WHERE source = 'booking' AND run_id = ?
            """
            df_prev = pd.read_sql_query(query, conn, params=(prev_run,))
            df_curr = pd.read_sql_query(query, conn, params=(curr_run,))

            if df_prev.empty or df_curr.empty:
                logger.warning(
                    "One or both Booking.com snapshot sets are empty "
                    "(prev rows=%d, curr rows=%d)",
                    len(df_prev),
                    len(df_curr),
                )
                return 0

            merged = df_prev.merge(
                df_curr,
                on=["listing_id", "snapshot_date"],
                suffixes=("_prev", "_curr"),
                how="inner",
            )

            # Detect room count changes (available_rooms decreased = booked).
            # Skip rows where either side is NULL — we can't infer a
            # transition if we don't know the previous or current state.
            has_both = (
                merged["available_rooms_prev"].notna()
                & merged["available_rooms_curr"].notna()
            )
            null_count = len(merged) - has_both.sum()
            if null_count > 0:
                logger.warning(
                    "Dropped %d/%d Booking rows with NULL available_rooms",
                    null_count,
                    len(merged),
                )
            merged = merged[has_both].copy()
            merged["rooms_prev"] = merged["available_rooms_prev"].astype(int)
            merged["rooms_curr"] = merged["available_rooms_curr"].astype(int)

            transitions = merged[
                merged["rooms_prev"] != merged["rooms_curr"]
            ].copy()

            if transitions.empty:
                logger.info("No Booking.com room-count transitions detected.")
                return 0

            logger.info(
                "Found %d room-count changes across %d properties",
                len(transitions),
                transitions["listing_id"].nunique(),
            )

            transitions["transition"] = transitions.apply(
                lambda r: (
                    "available_to_blocked"
                    if r["rooms_curr"] < r["rooms_prev"]
                    else "blocked_to_available"
                ),
                axis=1,
            )

            detected_at = now_iso()
            events = []
            for _, row in transitions.iterrows():
                lid = row["listing_id"]
                sdate = row["snapshot_date"]
                transition = row["transition"]
                zone_id = self._lookup_zone(conn, "booking", lid)

                # For Booking.com, classify bookings simply: room decreases
                # within a reasonable window are probable bookings.
                if transition == "available_to_blocked":
                    event_type = "probable_booking"
                else:
                    event_type = "unknown"

                last_price = (
                    row["price_prev"]
                    if pd.notna(row.get("price_prev"))
                    else None
                )
                currency = (
                    row["currency_prev"]
                    if pd.notna(row.get("currency_prev"))
                    else None
                )

                events.append(
                    (
                        "booking",
                        lid,
                        zone_id,
                        sdate,
                        detected_at,
                        prev_run,
                        curr_run,
                        transition,
                        event_type,
                        last_price,
                        currency,
                    )
                )

            if events:
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO occupancy_events
                        (source, listing_id, zone_id, event_date, detected_at,
                         prev_run_id, curr_run_id, transition, event_type,
                         last_known_price, currency)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    events,
                )
                conn.commit()

            logger.info(
                "Inserted %d Booking.com occupancy events.", len(events)
            )
            return len(events)

        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Blocked streak computation
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_blocked_streaks(
        blocked_df: pd.DataFrame,
    ) -> dict[tuple[str, str], int]:
        """Compute the length of each contiguous blocked streak.

        Given a DataFrame of blocked rows (is_available=0) with columns
        ``listing_id`` and ``snapshot_date``, return a mapping from
        (listing_id, snapshot_date) to the number of consecutive blocked
        days surrounding that date.

        The algorithm sorts dates per listing, detects contiguous groups
        (consecutive calendar days), and assigns each group its length.
        """
        if blocked_df.empty:
            return {}

        df = blocked_df[["listing_id", "snapshot_date"]].copy()
        df["date_dt"] = pd.to_datetime(df["snapshot_date"])
        df = df.sort_values(["listing_id", "date_dt"])

        result: dict[tuple[str, str], int] = {}

        for lid, group in df.groupby("listing_id"):
            dates = group["date_dt"].dt.date.tolist()
            if not dates:
                continue

            # Walk through sorted dates and group contiguous stretches.
            streak_start = 0
            for i in range(1, len(dates)):
                if (dates[i] - dates[i - 1]).days > 1:
                    # Previous streak ends; record its length.
                    streak_len = i - streak_start
                    for j in range(streak_start, i):
                        result[(lid, dates[j].isoformat())] = streak_len
                    streak_start = i

            # Final streak
            streak_len = len(dates) - streak_start
            for j in range(streak_start, len(dates)):
                result[(lid, dates[j].isoformat())] = streak_len

        return result

    @staticmethod
    def _fetch_min_nights(
        conn: sqlite3.Connection, listing_ids: list[str]
    ) -> dict[str, Optional[int]]:
        """Fetch minimum_nights for a batch of Airbnb listings."""
        if not listing_ids:
            return {}

        placeholders = ",".join("?" for _ in listing_ids)
        rows = conn.execute(
            f"""
            SELECT listing_id, minimum_nights
            FROM airbnb_listings
            WHERE listing_id IN ({placeholders})
            """,
            listing_ids,
        ).fetchall()

        return {row["listing_id"]: row["minimum_nights"] for row in rows}

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(self, run_id: Optional[int] = None) -> dict[str, int]:
        """Process occupancy transitions for both sources.

        Args:
            run_id: If provided, compare this run with its predecessor.
                    If None, use the two most recent completed runs per
                    source.

        Returns:
            Dict with keys 'airbnb' and 'booking' mapping to the number
            of events inserted for each source.
        """
        logger.info("=== OccupancyEngine run starting ===")

        airbnb_count = self.process_airbnb_transitions(run_id)
        booking_count = self.process_booking_occupancy(run_id)

        logger.info(
            "=== OccupancyEngine run complete: "
            "airbnb=%d events, booking=%d events ===",
            airbnb_count,
            booking_count,
        )
        return {"airbnb": airbnb_count, "booking": booking_count}


# ---------------------------------------------------------------------------
# Standalone testing
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Run occupancy inference from calendar snapshot diffs."
    )
    parser.add_argument(
        "--run-id",
        type=int,
        default=None,
        help="Specific run_id to process. Omit to use the latest pair.",
    )
    parser.add_argument(
        "--source",
        choices=["airbnb", "booking", "both"],
        default="both",
        help="Which source to process (default: both).",
    )
    args = parser.parse_args()

    engine = OccupancyEngine()

    if args.source == "airbnb":
        n = engine.process_airbnb_transitions(args.run_id)
        print(f"Airbnb: {n} occupancy events inserted.")
    elif args.source == "booking":
        n = engine.process_booking_occupancy(args.run_id)
        print(f"Booking.com: {n} occupancy events inserted.")
    else:
        results = engine.run(args.run_id)
        print(
            f"Airbnb: {results['airbnb']} events | "
            f"Booking.com: {results['booking']} events"
        )
