-- =============================================================================
-- Lombok Market Intelligence — SQLite Database Schema
-- Purpose: Track Airbnb and Booking.com listings, availability, and pricing
--          for real estate investment analysis in Lombok, Indonesia.
-- Design:  Analytics-first. Calendar snapshots are the high-volume table.
--          Normalized where it reduces redundancy; denormalized where it aids
--          query performance for time-series and zone-aggregate workloads.
-- =============================================================================

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = -32000; -- 32MB page cache


-- =============================================================================
-- REFERENCE TABLES
-- =============================================================================

-- Geographic investment zones for Lombok.
-- Each listing is assigned a zone based on lat/lng at scrape time.
-- Bounding boxes are approximate and may overlap at edges; zone_priority
-- resolves conflicts (lower number wins).
CREATE TABLE IF NOT EXISTS zones (
    zone_id         TEXT        PRIMARY KEY,        -- e.g. 'KUT', 'GLI'
    name            TEXT        NOT NULL,
    description     TEXT,
    lat_min         REAL        NOT NULL,
    lat_max         REAL        NOT NULL,
    lng_min         REAL        NOT NULL,
    lng_max         REAL        NOT NULL,
    zone_priority   INTEGER     NOT NULL DEFAULT 50, -- lower = checked first on overlap
    created_at      TEXT        NOT NULL DEFAULT (datetime('now'))
);

-- Bounding boxes derived from OpenStreetMap / local knowledge.
-- Lombok spans roughly -9.1 to -8.1 lat, 115.9 to 116.8 lng.
INSERT OR IGNORE INTO zones (zone_id, name, description, lat_min, lat_max, lng_min, lng_max, zone_priority) VALUES
    ('GLI', 'Gili Islands',               'Gili Trawangan, Meno, Air — offshore northwest Lombok', -8.38,  -8.30,  116.00, 116.10, 10),
    ('SGG', 'Senggigi',                   'Senggigi beach corridor, northwest coast',               -8.55,  -8.38,  116.01, 116.10, 20),
    ('NLB', 'North Lombok / Sire',        'Sire peninsula, Gondang, north coast',                  -8.38,  -8.15,  116.10, 116.45, 30),
    ('MTR', 'Mataram',                    'Urban capital — Mataram, Cakranegara, Ampenan',          -8.65,  -8.50,  116.05, 116.20, 40),
    ('KUT', 'Kuta / Mandalika',           'Kuta village, Mandalika SEZ, central south coast',      -8.92,  -8.80,  116.23, 116.38, 10),
    ('TAA', 'Tanjung Aan / Gerupuk',      'Tanjung Aan bay, Gerupuk surf bay, southeast of Kuta',  -8.92,  -8.82,  116.35, 116.52, 20),
    ('SBK', 'Selong Belanak / West Surf', 'Selong Belanak, Mawun, Mawi — west of Kuta',            -8.95,  -8.82,  116.05, 116.27, 20),
    ('SKT', 'Sekotong',                   'Sekotong peninsula, southwest Lombok, off-grid villas',  -8.95,  -8.60,  115.88, 116.08, 30);


-- =============================================================================
-- SCRAPE TRACKING
-- =============================================================================

-- One row per scrape run (scheduled or ad-hoc).
-- Used to correlate snapshots to a specific crawl, audit data freshness,
-- and monitor scraper health over time.
CREATE TABLE IF NOT EXISTS scrape_runs (
    run_id          INTEGER     PRIMARY KEY AUTOINCREMENT,
    source          TEXT        NOT NULL CHECK (source IN ('airbnb', 'booking')),
    run_type        TEXT        NOT NULL CHECK (run_type IN ('full', 'incremental', 'calendar_only', 'discovery')),
    started_at      TEXT        NOT NULL DEFAULT (datetime('now')),
    finished_at     TEXT,
    status          TEXT        NOT NULL DEFAULT 'running'
                                CHECK (status IN ('running', 'completed', 'partial', 'failed')),
    listings_seen   INTEGER     DEFAULT 0,
    listings_new    INTEGER     DEFAULT 0,
    snapshots_added INTEGER     DEFAULT 0,
    error_message   TEXT,
    notes           TEXT
);


-- =============================================================================
-- LISTING MASTER TABLES
-- =============================================================================

-- One row per unique Airbnb listing, updated in-place on re-scrape.
-- Pricing, ratings, and descriptions change over time but we store current
-- values here; historical price detail lives in calendar_snapshots.
CREATE TABLE IF NOT EXISTS airbnb_listings (
    listing_id          TEXT        PRIMARY KEY,    -- Airbnb's numeric ID stored as text
    url                 TEXT        NOT NULL,
    name                TEXT,
    description         TEXT,

    -- Property classification
    property_type       TEXT,                       -- 'villa', 'apartment', 'house', 'hotel room', etc.
    room_type           TEXT,                       -- 'entire_home', 'private_room', 'shared_room'

    -- Location
    latitude            REAL,
    longitude           REAL,
    zone_id             TEXT        REFERENCES zones(zone_id),   -- assigned at scrape time
    neighborhood        TEXT,       -- Airbnb's neighborhood label if present

    -- Capacity
    accommodates        INTEGER,    -- max guests
    bedrooms            INTEGER,
    beds                INTEGER,
    bathrooms           REAL,       -- 1.5 = 1 full + 1 half

    -- Pricing (current, as of last_scraped_at)
    nightly_price       REAL,
    cleaning_fee        REAL,
    currency            TEXT        NOT NULL DEFAULT 'USD',

    -- Host
    host_id             TEXT,
    host_name           TEXT,
    is_superhost        INTEGER     NOT NULL DEFAULT 0 CHECK (is_superhost IN (0, 1)),

    -- Ratings (0–5 scale, NULL if no reviews yet)
    rating_overall      REAL,
    rating_accuracy     REAL,
    rating_checkin      REAL,
    rating_cleanliness  REAL,
    rating_communication REAL,
    rating_location     REAL,
    rating_value        REAL,
    review_count        INTEGER     DEFAULT 0,
    reviews_per_month   REAL,

    -- Booking rules
    instant_bookable    INTEGER     NOT NULL DEFAULT 0 CHECK (instant_bookable IN (0, 1)),
    minimum_nights      INTEGER     DEFAULT 1,
    maximum_nights      INTEGER,

    -- Scrape metadata
    first_scraped_at    TEXT        NOT NULL DEFAULT (datetime('now')),
    last_scraped_at     TEXT        NOT NULL DEFAULT (datetime('now')),
    is_active           INTEGER     NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    last_run_id         INTEGER     REFERENCES scrape_runs(run_id)
);

-- Amenities for Airbnb listings (one row per amenity per listing).
-- Kept separate to avoid comma-delimited blobs; allows amenity filtering.
CREATE TABLE IF NOT EXISTS airbnb_amenities (
    listing_id  TEXT    NOT NULL REFERENCES airbnb_listings(listing_id) ON DELETE CASCADE,
    amenity     TEXT    NOT NULL,
    PRIMARY KEY (listing_id, amenity)
);

-- One row per unique Booking.com property, updated in-place on re-scrape.
CREATE TABLE IF NOT EXISTS booking_listings (
    property_id         TEXT        PRIMARY KEY,    -- Booking.com property ID
    url                 TEXT        NOT NULL,
    name                TEXT,

    -- Property classification
    property_type       TEXT,                       -- 'hotel', 'villa', 'apartment', 'guesthouse', etc.
    star_rating         INTEGER     CHECK (star_rating BETWEEN 0 AND 5),

    -- Location
    latitude            REAL,
    longitude           REAL,
    zone_id             TEXT        REFERENCES zones(zone_id),
    neighborhood        TEXT,

    -- Ratings (Booking uses 0–10)
    review_score        REAL,       -- out of 10
    review_count        INTEGER     DEFAULT 0,

    -- Scrape metadata
    first_scraped_at    TEXT        NOT NULL DEFAULT (datetime('now')),
    last_scraped_at     TEXT        NOT NULL DEFAULT (datetime('now')),
    is_active           INTEGER     NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    last_run_id         INTEGER     REFERENCES scrape_runs(run_id)
);

-- Room types for Booking.com properties (one property has multiple room categories).
CREATE TABLE IF NOT EXISTS booking_room_types (
    room_type_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id     TEXT    NOT NULL REFERENCES booking_listings(property_id) ON DELETE CASCADE,
    room_name       TEXT    NOT NULL,
    max_occupancy   INTEGER,
    bed_type        TEXT,
    is_active       INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    first_seen_at   TEXT    NOT NULL DEFAULT (datetime('now')),
    last_seen_at    TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE (property_id, room_name)
);

-- Amenities for Booking.com properties.
CREATE TABLE IF NOT EXISTS booking_amenities (
    property_id TEXT    NOT NULL REFERENCES booking_listings(property_id) ON DELETE CASCADE,
    amenity     TEXT    NOT NULL,
    PRIMARY KEY (property_id, amenity)
);


-- =============================================================================
-- CALENDAR / AVAILABILITY SNAPSHOTS
-- =============================================================================

-- High-volume table. Each scrape run records one row per listing per future
-- calendar date. For a full scrape of 500 listings × 90 days = 45,000 rows
-- per run. Index design is critical here.
--
-- Fields:
--   snapshot_date  : the calendar date this row describes (the "night" being sold)
--   scraped_at     : when this snapshot was recorded
--   is_available   : 1 = bookable, 0 = blocked/booked
--   price          : nightly rate offered on that date (NULL if unavailable)
--   available_rooms: Booking.com multi-room count (NULL for Airbnb / single-unit)
--
-- Occupancy inference: compare consecutive snapshots for the same listing+date.
-- If is_available flipped 1→0 between two scrape runs, flag as probable_booking.
CREATE TABLE IF NOT EXISTS calendar_snapshots (
    snapshot_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT    NOT NULL CHECK (source IN ('airbnb', 'booking')),
    listing_id      TEXT    NOT NULL,   -- FK enforced at app layer (polymorphic source)
    run_id          INTEGER NOT NULL REFERENCES scrape_runs(run_id),

    snapshot_date   TEXT    NOT NULL,   -- ISO-8601 date: 'YYYY-MM-DD' (the night)
    scraped_at      TEXT    NOT NULL DEFAULT (datetime('now')),

    is_available    INTEGER NOT NULL CHECK (is_available IN (0, 1)),
    price           REAL,               -- nightly price in listing currency; NULL if blocked
    currency        TEXT,
    available_rooms INTEGER,            -- Booking.com only; NULL for Airbnb

    UNIQUE (source, listing_id, snapshot_date, run_id)
);

-- Inferred booking events derived from calendar diff processing.
-- Populated by a post-scrape job that compares consecutive snapshots.
-- Each row represents a probable booking (or owner-block) transition.
CREATE TABLE IF NOT EXISTS occupancy_events (
    event_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source              TEXT    NOT NULL CHECK (source IN ('airbnb', 'booking')),
    listing_id          TEXT    NOT NULL,
    zone_id             TEXT    REFERENCES zones(zone_id),

    event_date          TEXT    NOT NULL,   -- the calendar night that changed state
    detected_at         TEXT    NOT NULL,   -- when the transition was detected
    prev_run_id         INTEGER REFERENCES scrape_runs(run_id),
    curr_run_id         INTEGER REFERENCES scrape_runs(run_id),

    -- Transition direction
    transition          TEXT    NOT NULL CHECK (transition IN ('available_to_blocked', 'blocked_to_available')),
    -- Classification: probable_booking when available→blocked near-term;
    -- owner_block when blocked well in advance with no price history.
    event_type          TEXT    NOT NULL DEFAULT 'unknown'
                                CHECK (event_type IN ('probable_booking', 'owner_block', 'unknown')),

    -- Price at time of last available snapshot (for ADR computation)
    last_known_price    REAL,
    currency            TEXT
);

-- Idempotency: prevent duplicate events from re-running on same run pair
-- (applied via _apply_migrations for existing DBs)
CREATE UNIQUE INDEX IF NOT EXISTS uix_occupancy_events_dedup
    ON occupancy_events (source, listing_id, event_date, prev_run_id, curr_run_id);


-- =============================================================================
-- PRICE HISTORY (point-in-time listing-level pricing)
-- =============================================================================

-- Captures the base nightly price of a listing each time a scrape run
-- updates it. Enables forward rate curve analysis and price trend queries
-- independent of specific calendar dates.
CREATE TABLE IF NOT EXISTS price_history (
    history_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT    NOT NULL CHECK (source IN ('airbnb', 'booking')),
    listing_id      TEXT    NOT NULL,
    room_type_id    INTEGER REFERENCES booking_room_types(room_type_id),  -- Booking.com only
    run_id          INTEGER NOT NULL REFERENCES scrape_runs(run_id),
    recorded_at     TEXT    NOT NULL DEFAULT (datetime('now')),
    nightly_price   REAL    NOT NULL,
    cleaning_fee    REAL,
    currency        TEXT    NOT NULL DEFAULT 'USD'
);


-- =============================================================================
-- INDEXES
-- =============================================================================

-- scrape_runs
CREATE INDEX IF NOT EXISTS idx_scrape_runs_source_started
    ON scrape_runs (source, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_scrape_runs_status
    ON scrape_runs (status, started_at DESC);

-- airbnb_listings
CREATE INDEX IF NOT EXISTS idx_airbnb_zone
    ON airbnb_listings (zone_id);

CREATE INDEX IF NOT EXISTS idx_airbnb_zone_type
    ON airbnb_listings (zone_id, property_type);

CREATE INDEX IF NOT EXISTS idx_airbnb_location
    ON airbnb_listings (latitude, longitude);

CREATE INDEX IF NOT EXISTS idx_airbnb_active_zone
    ON airbnb_listings (is_active, zone_id);

-- booking_listings
CREATE INDEX IF NOT EXISTS idx_booking_zone
    ON booking_listings (zone_id);

CREATE INDEX IF NOT EXISTS idx_booking_zone_type
    ON booking_listings (zone_id, property_type);

CREATE INDEX IF NOT EXISTS idx_booking_location
    ON booking_listings (latitude, longitude);

CREATE INDEX IF NOT EXISTS idx_booking_active_zone
    ON booking_listings (is_active, zone_id);

-- calendar_snapshots — the most-queried table
CREATE INDEX IF NOT EXISTS idx_cal_source_listing_date
    ON calendar_snapshots (source, listing_id, snapshot_date);

CREATE INDEX IF NOT EXISTS idx_cal_date_source
    ON calendar_snapshots (snapshot_date, source);

CREATE INDEX IF NOT EXISTS idx_cal_run_id
    ON calendar_snapshots (run_id);

CREATE INDEX IF NOT EXISTS idx_cal_available_date
    ON calendar_snapshots (is_available, snapshot_date);

-- occupancy_events
CREATE INDEX IF NOT EXISTS idx_occ_listing_date
    ON occupancy_events (source, listing_id, event_date);

CREATE INDEX IF NOT EXISTS idx_occ_zone_date
    ON occupancy_events (zone_id, event_date);

CREATE INDEX IF NOT EXISTS idx_occ_event_type_date
    ON occupancy_events (event_type, event_date);

-- price_history
CREATE INDEX IF NOT EXISTS idx_price_source_listing_recorded
    ON price_history (source, listing_id, recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_price_run_id
    ON price_history (run_id);


-- =============================================================================
-- VIEWS — pre-built lenses for common analytics queries
-- =============================================================================

-- Combined listing view across both sources.
-- Use this as the starting point for zone-level supply analysis.
CREATE VIEW IF NOT EXISTS v_all_listings AS
SELECT
    'airbnb'                AS source,
    listing_id              AS property_id,
    name,
    property_type,
    room_type,
    NULL                    AS star_rating,
    latitude,
    longitude,
    zone_id,
    accommodates,
    bedrooms,
    nightly_price,
    currency,
    rating_overall          AS rating,       -- Airbnb 0-5
    NULL                    AS review_score, -- Booking.com 0-10 equivalent
    review_count,
    is_active,
    first_scraped_at,
    last_scraped_at
FROM airbnb_listings

UNION ALL

SELECT
    'booking'               AS source,
    property_id,
    name,
    property_type,
    NULL                    AS room_type,
    star_rating,
    latitude,
    longitude,
    zone_id,
    NULL                    AS accommodates,
    NULL                    AS bedrooms,
    NULL                    AS nightly_price,
    NULL                    AS currency,
    NULL                    AS rating,
    review_score,
    review_count,
    is_active,
    first_scraped_at,
    last_scraped_at
FROM booking_listings;


-- Active listing count and type breakdown per zone.
-- Quick supply snapshot. JOIN with zones for zone names.
CREATE VIEW IF NOT EXISTS v_supply_by_zone AS
SELECT
    l.zone_id,
    z.name                              AS zone_name,
    l.source,
    l.property_type,
    COUNT(*)                            AS listing_count,
    AVG(l.nightly_price)                AS avg_list_price,
    MIN(l.first_scraped_at)             AS earliest_listing,
    MAX(l.first_scraped_at)             AS newest_listing
FROM v_all_listings l
LEFT JOIN zones z ON z.zone_id = l.zone_id
WHERE l.is_active = 1
GROUP BY l.zone_id, l.source, l.property_type;


-- Occupancy rate computed from probable_booking events.
-- One row per (source, zone, year-month). Use for seasonality charting.
-- Note: denominator uses distinct listing-nights, not total capacity.
CREATE VIEW IF NOT EXISTS v_occupancy_monthly AS
SELECT
    oe.source,
    oe.zone_id,
    z.name                                          AS zone_name,
    strftime('%Y-%m', oe.event_date)                AS year_month,
    COUNT(*)                                        AS booked_nights,
    COUNT(DISTINCT oe.listing_id)                   AS unique_listings_booked,
    AVG(oe.last_known_price)                        AS avg_booked_price
FROM occupancy_events oe
LEFT JOIN zones z ON z.zone_id = oe.zone_id
WHERE oe.event_type = 'probable_booking'
GROUP BY oe.source, oe.zone_id, strftime('%Y-%m', oe.event_date);


-- ADR view with deduplication (no PERCENTILE — use v_adr_simple instead).
-- Deduplicates overlapping scrape runs: keeps only the latest run per
-- (source, listing_id, snapshot_date) to avoid double-counting.
CREATE VIEW IF NOT EXISTS v_adr_simple AS
WITH deduped AS (
    SELECT cs.*,
           ROW_NUMBER() OVER (
               PARTITION BY cs.source, cs.listing_id, cs.snapshot_date
               ORDER BY cs.run_id DESC
           ) AS rn
    FROM calendar_snapshots cs
)
SELECT
    cs.source,
    COALESCE(al.zone_id, bl.zone_id)               AS zone_id,
    z.name                                          AS zone_name,
    strftime('%Y-%m', cs.snapshot_date)             AS year_month,
    COUNT(*)                                        AS priced_night_obs,
    AVG(cs.price)                                   AS adr,
    MIN(cs.price)                                   AS min_nightly,
    MAX(cs.price)                                   AS max_nightly
FROM deduped cs
LEFT JOIN airbnb_listings  al ON cs.source = 'airbnb'  AND cs.listing_id = al.listing_id
LEFT JOIN booking_listings bl ON cs.source = 'booking' AND cs.listing_id = bl.property_id
LEFT JOIN zones z
       ON z.zone_id = COALESCE(al.zone_id, bl.zone_id)
WHERE cs.rn = 1
  AND cs.is_available = 1
  AND cs.price IS NOT NULL
GROUP BY cs.source, COALESCE(al.zone_id, bl.zone_id), strftime('%Y-%m', cs.snapshot_date);


-- Forward rate curve: average listed price per day for the next 90 days.
-- Run this query live (not as a view) filtered by run_id = latest run.
-- Provided here as a view using latest snapshot per listing/date.
CREATE VIEW IF NOT EXISTS v_forward_rates AS
SELECT
    cs.source,
    COALESCE(al.zone_id, bl.zone_id)               AS zone_id,
    cs.snapshot_date,
    COUNT(*)                                        AS listings_priced,
    AVG(cs.price)                                   AS avg_price,
    MIN(cs.price)                                   AS min_price,
    MAX(cs.price)                                   AS max_price
FROM calendar_snapshots cs
-- Use only the most recent snapshot per listing/date
INNER JOIN (
    SELECT source, listing_id, snapshot_date, MAX(run_id) AS latest_run_id
    FROM calendar_snapshots
    GROUP BY source, listing_id, snapshot_date
) latest ON latest.source    = cs.source
         AND latest.listing_id   = cs.listing_id
         AND latest.snapshot_date = cs.snapshot_date
         AND latest.latest_run_id = cs.run_id
LEFT JOIN airbnb_listings  al ON cs.source = 'airbnb'  AND cs.listing_id = al.listing_id
LEFT JOIN booking_listings bl ON cs.source = 'booking' AND cs.listing_id = bl.property_id
WHERE cs.is_available = 1
  AND cs.price IS NOT NULL
  AND cs.snapshot_date >= date('now')
GROUP BY cs.source, COALESCE(al.zone_id, bl.zone_id), cs.snapshot_date;


-- Supply growth: cumulative new listings per zone by month.
CREATE VIEW IF NOT EXISTS v_supply_growth AS
SELECT
    l.source,
    l.zone_id,
    z.name                                          AS zone_name,
    strftime('%Y-%m', l.first_scraped_at)           AS first_seen_month,
    COUNT(*)                                        AS new_listings,
    SUM(COUNT(*)) OVER (
        PARTITION BY l.source, l.zone_id
        ORDER BY strftime('%Y-%m', l.first_scraped_at)
    )                                               AS cumulative_listings
FROM v_all_listings l
LEFT JOIN zones z ON z.zone_id = l.zone_id
GROUP BY l.source, l.zone_id, strftime('%Y-%m', l.first_scraped_at);


-- RevPAR = ADR × occupancy rate.
-- Deduplicates overlapping scrape runs before computing metrics.
CREATE VIEW IF NOT EXISTS v_revpar_monthly AS
WITH deduped AS (
    SELECT cs.*,
           ROW_NUMBER() OVER (
               PARTITION BY cs.source, cs.listing_id, cs.snapshot_date
               ORDER BY cs.run_id DESC
           ) AS rn
    FROM calendar_snapshots cs
    WHERE cs.snapshot_date < date('now')
),
avail AS (
    SELECT
        cs.source,
        COALESCE(al.zone_id, bl.zone_id)           AS zone_id,
        strftime('%Y-%m', cs.snapshot_date)         AS year_month,
        COUNT(*)                                    AS total_night_obs,
        SUM(CASE WHEN cs.is_available = 0 THEN 1 ELSE 0 END) AS blocked_nights,
        AVG(CASE WHEN cs.is_available = 1 AND cs.price IS NOT NULL
                 THEN cs.price END)                 AS adr
    FROM deduped cs
    LEFT JOIN airbnb_listings  al ON cs.source = 'airbnb'  AND cs.listing_id = al.listing_id
    LEFT JOIN booking_listings bl ON cs.source = 'booking' AND cs.listing_id = bl.property_id
    WHERE cs.rn = 1
    GROUP BY cs.source, COALESCE(al.zone_id, bl.zone_id), strftime('%Y-%m', cs.snapshot_date)
)
SELECT
    source,
    zone_id,
    year_month,
    adr,
    MIN(CAST(blocked_nights AS REAL) / NULLIF(total_night_obs, 0), 1.0) AS occupancy_rate,
    adr * MIN(CAST(blocked_nights AS REAL) / NULLIF(total_night_obs, 0), 1.0) AS revpar
FROM avail;


-- Seasonality: aggregate by day-of-week and month across all history.
CREATE VIEW IF NOT EXISTS v_seasonality AS
SELECT
    cs.source,
    COALESCE(al.zone_id, bl.zone_id)               AS zone_id,
    CAST(strftime('%m', cs.snapshot_date) AS INTEGER) AS month_num,
    CAST(strftime('%w', cs.snapshot_date) AS INTEGER) AS day_of_week,  -- 0=Sun
    COUNT(*)                                        AS obs,
    AVG(cs.price)                                   AS avg_price,
    AVG(CASE WHEN cs.is_available = 0 THEN 1.0 ELSE 0.0 END) AS block_rate
FROM calendar_snapshots cs
LEFT JOIN airbnb_listings  al ON cs.source = 'airbnb'  AND cs.listing_id = al.listing_id
LEFT JOIN booking_listings bl ON cs.source = 'booking' AND cs.listing_id = bl.property_id
WHERE cs.price IS NOT NULL
GROUP BY cs.source, COALESCE(al.zone_id, bl.zone_id),
         strftime('%m', cs.snapshot_date), strftime('%w', cs.snapshot_date);


-- Latest scrape run summary per source.
CREATE VIEW IF NOT EXISTS v_scrape_health AS
SELECT
    source,
    MAX(run_id)                                     AS latest_run_id,
    MAX(started_at)                                 AS last_run_started,
    MAX(finished_at)                                AS last_run_finished,
    SUM(CASE WHEN status = 'completed' THEN 1 END)  AS completed_runs,
    SUM(CASE WHEN status = 'failed'    THEN 1 END)  AS failed_runs,
    SUM(CASE WHEN status = 'partial'   THEN 1 END)  AS partial_runs,
    SUM(listings_seen)                              AS total_listings_seen,
    SUM(snapshots_added)                            AS total_snapshots_added
FROM scrape_runs
GROUP BY source;


-- =============================================================================
-- END OF SCHEMA
-- =============================================================================
