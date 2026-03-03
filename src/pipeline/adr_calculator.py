"""ADR and derived metrics calculator for Lombok Market Intelligence.

Computes zone-level Average Daily Rate, occupancy rate, RevPAR, forward
rate curves, and seasonality indices from calendar snapshots and
occupancy events stored in the SQLite database.

Typical usage:
    calc = ADRCalculator()
    adr_df = calc.compute_zone_adr(zone_id="KUT")
    calc.run()  # compute all metrics and log summary
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime
from typing import Optional

import numpy as np
import pandas as pd

from src.config import DB_PATH, CALENDAR_DAYS_FORWARD
from src.utils import setup_logger, now_iso
from src.db.init_db import get_connection

logger = setup_logger("adr_calculator")

# Forward-curve horizons (days from today)
FORWARD_HORIZONS = [1, 7, 14, 30, 60, 90]


class ADRCalculator:
    """Compute ADR, occupancy, RevPAR, forward curves, and seasonality."""

    def __init__(self, db_path=None):
        self.db_path = db_path or DB_PATH
        logger.info("ADRCalculator initialised (db=%s)", self.db_path)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_conn(self) -> sqlite3.Connection:
        return get_connection(self.db_path)

    @staticmethod
    def _zone_filter(zone_id: Optional[str], alias: str = "zone_id") -> str:
        """Return a SQL fragment to filter by zone, or '1=1' if None."""
        if zone_id is None:
            return "1=1"
        return f"{alias} = :zone_id"

    @staticmethod
    def _date_filter(
        start_date: Optional[str],
        end_date: Optional[str],
        date_col: str,
    ) -> str:
        """Return a SQL fragment for optional date range filtering."""
        clauses = []
        if start_date is not None:
            clauses.append(f"{date_col} >= :start_date")
        if end_date is not None:
            clauses.append(f"{date_col} <= :end_date")
        return " AND ".join(clauses) if clauses else "1=1"

    @staticmethod
    def _build_params(
        zone_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> dict:
        params: dict = {}
        if zone_id is not None:
            params["zone_id"] = zone_id
        if start_date is not None:
            params["start_date"] = start_date
        if end_date is not None:
            params["end_date"] = end_date
        return params

    # ------------------------------------------------------------------
    # 1. ADR by zone and month
    # ------------------------------------------------------------------

    def compute_zone_adr(
        self,
        zone_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Compute Average Daily Rate per zone per month.

        Only considers calendar snapshots where is_available=1 and price
        is not null (i.e. listed, bookable nights).

        Args:
            zone_id: Filter to a single zone. None for all zones.
            start_date: ISO date lower bound (inclusive) on snapshot_date.
            end_date: ISO date upper bound (inclusive) on snapshot_date.

        Returns:
            DataFrame with columns: zone_id, year_month, adr, min_price,
            max_price, sample_size.
        """
        conn = self._get_conn()
        try:
            zone_clause = self._zone_filter(
                zone_id, "COALESCE(al.zone_id, bl.zone_id)"
            )
            date_clause = self._date_filter(
                start_date, end_date, "cs.snapshot_date"
            )

            # Deduplicate: keep only the latest run's snapshot per
            # (source, listing_id, snapshot_date) to avoid double-counting
            # when multiple scrape runs cover the same dates.
            # ROW_NUMBER runs on ALL snapshots (not pre-filtered) so the
            # latest run always wins regardless of availability state.
            dedup_cte = f"""
                WITH deduped AS (
                    SELECT cs.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY cs.source, cs.listing_id, cs.snapshot_date
                               ORDER BY cs.run_id DESC
                           ) AS rn
                    FROM calendar_snapshots cs
                    WHERE {date_clause}
                )
            """

            sql = f"""
                {dedup_cte}
                SELECT
                    COALESCE(al.zone_id, bl.zone_id) AS zone_id,
                    strftime('%Y-%m', cs.snapshot_date) AS year_month,
                    AVG(cs.price)   AS adr,
                    MIN(cs.price)   AS min_price,
                    MAX(cs.price)   AS max_price,
                    COUNT(*)        AS sample_size
                FROM deduped cs
                LEFT JOIN airbnb_listings  al
                    ON cs.source = 'airbnb' AND cs.listing_id = al.listing_id
                LEFT JOIN booking_listings bl
                    ON cs.source = 'booking' AND cs.listing_id = bl.property_id
                WHERE cs.rn = 1
                  AND cs.is_available = 1
                  AND cs.price IS NOT NULL
                  AND {zone_clause}
                GROUP BY COALESCE(al.zone_id, bl.zone_id),
                         strftime('%Y-%m', cs.snapshot_date)
                ORDER BY zone_id, year_month
            """
            params = self._build_params(zone_id, start_date, end_date)
            df = pd.read_sql_query(sql, conn, params=params)

            # Add percentile columns from raw price data (also deduped)
            if not df.empty:
                pct_sql = f"""
                    {dedup_cte}
                    SELECT
                        COALESCE(al.zone_id, bl.zone_id) AS zone_id,
                        strftime('%Y-%m', cs.snapshot_date) AS year_month,
                        cs.price
                    FROM deduped cs
                    LEFT JOIN airbnb_listings  al
                        ON cs.source = 'airbnb' AND cs.listing_id = al.listing_id
                    LEFT JOIN booking_listings bl
                        ON cs.source = 'booking' AND cs.listing_id = bl.property_id
                    WHERE cs.rn = 1
                      AND cs.is_available = 1
                      AND cs.price IS NOT NULL
                      AND {zone_clause}
                """
                df_raw = pd.read_sql_query(pct_sql, conn, params=params)
                if not df_raw.empty:
                    pcts = (
                        df_raw.groupby(["zone_id", "year_month"])["price"]
                        .quantile([0.25, 0.5, 0.75])
                        .unstack()
                        .reset_index()
                    )
                    pcts.columns = ["zone_id", "year_month", "p25", "median", "p75"]
                    df = df.merge(pcts, on=["zone_id", "year_month"], how="left")

                # MoM growth rate
                df = df.sort_values(["zone_id", "year_month"])
                df["adr_mom_pct"] = df.groupby("zone_id")["adr"].pct_change() * 100

            logger.info(
                "compute_zone_adr: %d rows (zone=%s, %s to %s)",
                len(df),
                zone_id or "ALL",
                start_date or "earliest",
                end_date or "latest",
            )
            return df

        finally:
            conn.close()

    # ------------------------------------------------------------------
    # 2. Occupancy rate by zone and month
    # ------------------------------------------------------------------

    def compute_zone_occupancy(
        self,
        zone_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Compute occupancy rate per zone per month from occupancy events.

        Occupancy rate = booked_nights / total_listing_nights where
        total_listing_nights is estimated from the distinct listing-date
        pairs observed in calendar_snapshots for the same zone and period.

        Args:
            zone_id: Filter to a single zone. None for all zones.
            start_date: ISO date lower bound (inclusive).
            end_date: ISO date upper bound (inclusive).

        Returns:
            DataFrame with columns: zone_id, year_month, occupancy_rate,
            booked_nights, total_nights.
        """
        conn = self._get_conn()
        try:
            zone_clause_oe = self._zone_filter(zone_id, "oe.zone_id")
            date_clause_oe = self._date_filter(
                start_date, end_date, "oe.event_date"
            )
            zone_clause_cs = self._zone_filter(
                zone_id, "COALESCE(al.zone_id, bl.zone_id)"
            )
            date_clause_cs = self._date_filter(
                start_date, end_date, "cs.snapshot_date"
            )

            # Booked nights from occupancy_events (probable_booking only)
            sql_booked = f"""
                SELECT
                    oe.zone_id,
                    strftime('%Y-%m', oe.event_date) AS year_month,
                    COUNT(*) AS booked_nights
                FROM occupancy_events oe
                WHERE oe.event_type = 'probable_booking'
                  AND {zone_clause_oe}
                  AND {date_clause_oe}
                GROUP BY oe.zone_id, strftime('%Y-%m', oe.event_date)
            """
            params = self._build_params(zone_id, start_date, end_date)
            df_booked = pd.read_sql_query(sql_booked, conn, params=params)

            # Total listing-nights: distinct (listing_id, snapshot_date)
            # per zone and month from calendar_snapshots (most recent run
            # per listing/date to avoid double-counting).
            sql_total = f"""
                SELECT
                    COALESCE(al.zone_id, bl.zone_id) AS zone_id,
                    strftime('%Y-%m', cs.snapshot_date) AS year_month,
                    COUNT(DISTINCT cs.source || '|' || cs.listing_id || '|' || cs.snapshot_date)
                        AS total_nights
                FROM calendar_snapshots cs
                LEFT JOIN airbnb_listings  al
                    ON cs.source = 'airbnb' AND cs.listing_id = al.listing_id
                LEFT JOIN booking_listings bl
                    ON cs.source = 'booking' AND cs.listing_id = bl.property_id
                WHERE {zone_clause_cs}
                  AND {date_clause_cs}
                GROUP BY COALESCE(al.zone_id, bl.zone_id),
                         strftime('%Y-%m', cs.snapshot_date)
            """
            df_total = pd.read_sql_query(sql_total, conn, params=params)

            if df_booked.empty and df_total.empty:
                logger.warning(
                    "compute_zone_occupancy: no data for zone=%s",
                    zone_id or "ALL",
                )
                return pd.DataFrame(
                    columns=[
                        "zone_id",
                        "year_month",
                        "occupancy_rate",
                        "booked_nights",
                        "total_nights",
                    ]
                )

            # Merge
            df = df_total.merge(
                df_booked, on=["zone_id", "year_month"], how="left"
            )
            df["booked_nights"] = df["booked_nights"].fillna(0).astype(int)
            df["occupancy_rate"] = (
                df["booked_nights"] / df["total_nights"].replace(0, pd.NA)
            ).clip(upper=1.0)

            df = df.sort_values(["zone_id", "year_month"]).reset_index(
                drop=True
            )

            logger.info(
                "compute_zone_occupancy: %d rows (zone=%s)",
                len(df),
                zone_id or "ALL",
            )
            return df

        finally:
            conn.close()

    # ------------------------------------------------------------------
    # 3. RevPAR
    # ------------------------------------------------------------------

    def compute_revpar(
        self,
        zone_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Compute RevPAR (Revenue Per Available Room-night) per zone/month.

        RevPAR = ADR * occupancy_rate.

        Returns:
            DataFrame with columns: zone_id, year_month, adr,
            occupancy_rate, revpar, booked_nights, total_nights,
            sample_size.
        """
        df_adr = self.compute_zone_adr(zone_id, start_date, end_date)
        df_occ = self.compute_zone_occupancy(zone_id, start_date, end_date)

        if df_adr.empty or df_occ.empty:
            logger.warning(
                "compute_revpar: insufficient data (adr rows=%d, occ rows=%d)",
                len(df_adr),
                len(df_occ),
            )
            return pd.DataFrame(
                columns=[
                    "zone_id",
                    "year_month",
                    "adr",
                    "occupancy_rate",
                    "revpar",
                    "booked_nights",
                    "total_nights",
                    "sample_size",
                ]
            )

        df = df_adr.merge(df_occ, on=["zone_id", "year_month"], how="outer")
        df["revpar"] = df["adr"] * df["occupancy_rate"]

        df = df.sort_values(["zone_id", "year_month"]).reset_index(drop=True)
        logger.info(
            "compute_revpar: %d rows (zone=%s)", len(df), zone_id or "ALL"
        )
        return df

    # ------------------------------------------------------------------
    # 4. Forward rate curve
    # ------------------------------------------------------------------

    def compute_forward_curve(
        self,
        zone_id: Optional[str] = None,
    ) -> pd.DataFrame:
        """Compute the forward price curve at standard horizons.

        For each horizon (1, 7, 14, 30, 60, 90 days out), average the
        listed price across all available listings in the zone using the
        most recent calendar snapshot per listing/date.

        Args:
            zone_id: Filter to a single zone. None for all zones.

        Returns:
            DataFrame with columns: zone_id, days_out, avg_price,
            min_price, max_price, listings_count.
        """
        conn = self._get_conn()
        try:
            today = date.today().isoformat()
            zone_clause = self._zone_filter(
                zone_id, "COALESCE(al.zone_id, bl.zone_id)"
            )

            # Use the most recent snapshot per (listing, date) only.
            sql = f"""
                WITH latest_snap AS (
                    SELECT
                        cs.source,
                        cs.listing_id,
                        cs.snapshot_date,
                        cs.price,
                        cs.is_available,
                        ROW_NUMBER() OVER (
                            PARTITION BY cs.source, cs.listing_id, cs.snapshot_date
                            ORDER BY cs.run_id DESC
                        ) AS rn
                    FROM calendar_snapshots cs
                    WHERE cs.snapshot_date >= :today
                      AND cs.is_available = 1
                      AND cs.price IS NOT NULL
                )
                SELECT
                    COALESCE(al.zone_id, bl.zone_id) AS zone_id,
                    CAST(julianday(ls.snapshot_date) - julianday(:today) AS INTEGER)
                        AS days_out,
                    AVG(ls.price)                   AS avg_price,
                    MIN(ls.price)                   AS min_price,
                    MAX(ls.price)                   AS max_price,
                    COUNT(DISTINCT ls.listing_id)   AS listings_count
                FROM latest_snap ls
                LEFT JOIN airbnb_listings  al
                    ON ls.source = 'airbnb' AND ls.listing_id = al.listing_id
                LEFT JOIN booking_listings bl
                    ON ls.source = 'booking' AND ls.listing_id = bl.property_id
                WHERE ls.rn = 1
                  AND {zone_clause}
                GROUP BY COALESCE(al.zone_id, bl.zone_id),
                         CAST(julianday(ls.snapshot_date) - julianday(:today) AS INTEGER)
                ORDER BY zone_id, days_out
            """
            params: dict = {"today": today}
            if zone_id is not None:
                params["zone_id"] = zone_id

            df_raw = pd.read_sql_query(sql, conn, params=params)

            if df_raw.empty:
                logger.warning(
                    "compute_forward_curve: no forward pricing data "
                    "(zone=%s)",
                    zone_id or "ALL",
                )
                return pd.DataFrame(
                    columns=[
                        "zone_id",
                        "days_out",
                        "avg_price",
                        "min_price",
                        "max_price",
                        "listings_count",
                    ]
                )

            # Bucket into the standard horizons by finding, for each zone,
            # the data point closest to each horizon.
            results = []
            for z, grp in df_raw.groupby("zone_id"):
                for horizon in FORWARD_HORIZONS:
                    # Select all rows within +/- 3 days of the horizon,
                    # then average them to smooth noise.
                    mask = (grp["days_out"] >= max(0, horizon - 3)) & (
                        grp["days_out"] <= horizon + 3
                    )
                    bucket = grp[mask]
                    if bucket.empty:
                        continue
                    results.append(
                        {
                            "zone_id": z,
                            "days_out": horizon,
                            "avg_price": bucket["avg_price"].mean(),
                            "min_price": bucket["min_price"].min(),
                            "max_price": bucket["max_price"].max(),
                            "listings_count": int(
                                bucket["listings_count"].mean()
                            ),
                        }
                    )

            df = pd.DataFrame(results)
            logger.info(
                "compute_forward_curve: %d horizon points (zone=%s)",
                len(df),
                zone_id or "ALL",
            )
            return df

        finally:
            conn.close()

    # ------------------------------------------------------------------
    # 5. Seasonality index
    # ------------------------------------------------------------------

    def compute_seasonality(
        self,
        zone_id: Optional[str] = None,
    ) -> pd.DataFrame:
        """Compute seasonality indices for ADR and occupancy by month.

        The index is computed as (monthly average) / (annual mean), so
        a value of 1.2 means that month is 20% above the annual average.

        Args:
            zone_id: Filter to a single zone. None for all zones.

        Returns:
            DataFrame with columns: zone_id, month, avg_adr,
            avg_occupancy, adr_index, occupancy_index.
        """
        # Get ADR data with a month column
        df_adr = self.compute_zone_adr(zone_id=zone_id)
        df_occ = self.compute_zone_occupancy(zone_id=zone_id)

        if df_adr.empty:
            logger.warning(
                "compute_seasonality: no ADR data (zone=%s)",
                zone_id or "ALL",
            )
            return pd.DataFrame(
                columns=[
                    "zone_id",
                    "month",
                    "avg_adr",
                    "avg_occupancy",
                    "adr_index",
                    "occupancy_index",
                ]
            )

        # Extract calendar month from year_month
        df_adr["month"] = df_adr["year_month"].str[5:7].astype(int)
        monthly_adr = (
            df_adr.groupby(["zone_id", "month"])
            .agg(avg_adr=("adr", "mean"))
            .reset_index()
        )

        # Compute annual mean ADR per zone
        annual_adr = (
            monthly_adr.groupby("zone_id")["avg_adr"]
            .mean()
            .rename("annual_avg_adr")
        )
        monthly_adr = monthly_adr.merge(annual_adr, on="zone_id")
        monthly_adr["adr_index"] = (
            monthly_adr["avg_adr"] / monthly_adr["annual_avg_adr"]
        )

        # Occupancy seasonality
        if not df_occ.empty:
            df_occ["month"] = df_occ["year_month"].str[5:7].astype(int)
            monthly_occ = (
                df_occ.groupby(["zone_id", "month"])
                .agg(avg_occupancy=("occupancy_rate", "mean"))
                .reset_index()
            )
            annual_occ = (
                monthly_occ.groupby("zone_id")["avg_occupancy"]
                .mean()
                .rename("annual_avg_occ")
            )
            monthly_occ = monthly_occ.merge(annual_occ, on="zone_id")
            monthly_occ["occupancy_index"] = (
                monthly_occ["avg_occupancy"]
                / monthly_occ["annual_avg_occ"].replace(0, pd.NA)
            )
        else:
            monthly_occ = pd.DataFrame(
                columns=[
                    "zone_id",
                    "month",
                    "avg_occupancy",
                    "annual_avg_occ",
                    "occupancy_index",
                ]
            )

        # Merge ADR and occupancy seasonality
        df = monthly_adr.merge(
            monthly_occ[["zone_id", "month", "avg_occupancy", "occupancy_index"]],
            on=["zone_id", "month"],
            how="left",
        )

        # Keep only output columns
        df = df[
            [
                "zone_id",
                "month",
                "avg_adr",
                "avg_occupancy",
                "adr_index",
                "occupancy_index",
            ]
        ].sort_values(["zone_id", "month"]).reset_index(drop=True)

        logger.info(
            "compute_seasonality: %d rows (zone=%s)",
            len(df),
            zone_id or "ALL",
        )
        return df

    # ------------------------------------------------------------------
    # Run all metrics
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Compute all metrics and log a summary report."""
        logger.info("=== ADRCalculator full run starting ===")

        # 1. ADR
        df_adr = self.compute_zone_adr()
        if not df_adr.empty:
            for _, row in (
                df_adr.groupby("zone_id")
                .agg(
                    months=("year_month", "count"),
                    mean_adr=("adr", "mean"),
                    total_obs=("sample_size", "sum"),
                )
                .iterrows()
            ):
                logger.info(
                    "  ADR zone=%s: %d months, mean ADR=%.1f, obs=%d",
                    _,
                    row["months"],
                    row["mean_adr"],
                    row["total_obs"],
                )
        else:
            logger.warning("  ADR: no data")

        # 2. Occupancy
        df_occ = self.compute_zone_occupancy()
        if not df_occ.empty:
            for _, row in (
                df_occ.groupby("zone_id")
                .agg(
                    months=("year_month", "count"),
                    mean_occ=("occupancy_rate", "mean"),
                )
                .iterrows()
            ):
                logger.info(
                    "  Occupancy zone=%s: %d months, mean rate=%.2f",
                    _,
                    row["months"],
                    row["mean_occ"],
                )
        else:
            logger.warning("  Occupancy: no data")

        # 3. RevPAR
        df_revpar = self.compute_revpar()
        if not df_revpar.empty:
            for z, grp in df_revpar.groupby("zone_id"):
                mean_rp = grp["revpar"].mean()
                logger.info("  RevPAR zone=%s: mean=%.1f", z, mean_rp)
        else:
            logger.warning("  RevPAR: no data")

        # 4. Forward curve
        df_fwd = self.compute_forward_curve()
        if not df_fwd.empty:
            for z, grp in df_fwd.groupby("zone_id"):
                horizons = grp["days_out"].tolist()
                prices = grp["avg_price"].tolist()
                curve_str = ", ".join(
                    f"{d}d=${p:.0f}" for d, p in zip(horizons, prices)
                )
                logger.info("  Forward curve zone=%s: %s", z, curve_str)
        else:
            logger.warning("  Forward curve: no data")

        # 5. Seasonality
        df_season = self.compute_seasonality()
        if not df_season.empty:
            zones_with_season = df_season["zone_id"].nunique()
            logger.info(
                "  Seasonality: computed for %d zones", zones_with_season
            )
        else:
            logger.warning("  Seasonality: no data")

        logger.info("=== ADRCalculator full run complete ===")


# ---------------------------------------------------------------------------
# Standalone testing
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Compute ADR and derived metrics."
    )
    parser.add_argument(
        "--zone",
        type=str,
        default=None,
        help="Zone ID to compute for (e.g. KUT). Omit for all zones.",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date (YYYY-MM-DD) for filtering.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date (YYYY-MM-DD) for filtering.",
    )
    parser.add_argument(
        "--metric",
        choices=["adr", "occupancy", "revpar", "forward", "seasonality", "all"],
        default="all",
        help="Which metric to compute (default: all).",
    )
    args = parser.parse_args()

    calc = ADRCalculator()

    if args.metric == "adr":
        df = calc.compute_zone_adr(args.zone, args.start_date, args.end_date)
        print(df.to_string(index=False))

    elif args.metric == "occupancy":
        df = calc.compute_zone_occupancy(
            args.zone, args.start_date, args.end_date
        )
        print(df.to_string(index=False))

    elif args.metric == "revpar":
        df = calc.compute_revpar(args.zone, args.start_date, args.end_date)
        print(df.to_string(index=False))

    elif args.metric == "forward":
        df = calc.compute_forward_curve(args.zone)
        print(df.to_string(index=False))

    elif args.metric == "seasonality":
        df = calc.compute_seasonality(args.zone)
        print(df.to_string(index=False))

    else:
        calc.run()
