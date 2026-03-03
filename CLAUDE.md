# Lombok Market Intelligence

Short-term rental market intelligence for Lombok, Indonesia. Scrapes Airbnb and Booking.com, infers occupancy from calendar diffs, computes ADR/RevPAR/seasonality, and serves an interactive Streamlit dashboard with Folium maps.

## Quick Reference

```bash
source .venv/bin/activate          # Always activate venv first
python main.py                     # Full pipeline: scrape + analyze + excel export
python main.py --scrape            # Scrapers only
python main.py --analyze           # Analysis only
python main.py --dashboard         # Launch Streamlit dashboard
python -m src.export_excel         # Excel export only
python -m src.scrapers.airbnb_scraper  # Airbnb scraper standalone
python -m src.db.archive           # Archive snapshots >180 days
pytest tests/                      # Run test suite (32 tests)
```

## Project Structure

```
main.py                          # CLI orchestrator (--scrape, --analyze, --dashboard)
src/
  config.py                      # Zone bounding boxes, scrape delays, shared constants
  utils.py                       # setup_logger, assign_zone, rate_limit, now_iso,
                                 #   validate_coordinates, validate_price, notify_telegram
  export_excel.py                # DB → data/lombok_intel.xlsx (11 sheets)
  db/
    schema.sql                   # 12 tables, 19 indexes, 10 views
    init_db.py                   # init_database(), get_connection() — PRAGMA busy_timeout=30s
    archive.py                   # Move calendar_snapshots >180 days to data/archive/
  scrapers/
    airbnb_scraper.py            # AirbnbScraper — pyairbnb GraphQL v3 (tenacity retries)
    booking_scraper.py           # BookingScraper — Playwright + stealth (tenacity, UA rotation)
  pipeline/
    occupancy_engine.py          # OccupancyEngine — calendar diff → occupancy_events
    adr_calculator.py            # ADRCalculator — ADR p25/median/p75, MoM growth, forward curves
  dashboard/
    app.py                       # Streamlit + Folium interactive map dashboard (local DB only)
data/
  lombok_intel.db                # SQLite database (PROPRIETARY — never expose publicly)
  lombok_intel.xlsx              # Excel export (regenerated each run)
  lombok_zones.geojson           # 8 zone polygons for map rendering
  logs/                          # Rotating log files (5MB, 5 backups)
  archive/                       # Archived old calendar snapshots
tests/
  test_utils.py                  # 12 tests: zone assignment, validators
  test_occupancy.py              # 3 tests: occupancy event classification
  test_export.py                 # 2 tests: Excel output verification
  test_scrapers.py               # 10 tests: safe_float, safe_int, bool_to_int
  test_hardening.py              # 5 tests: ADR dedup, migration idempotency, occupancy cap
docs/
  PRD.md                         # Full product requirements document
.github/workflows/
  scrape.yml                     # Daily GitHub Actions pipeline (2 AM UTC)
```

## Data Protection

**The database contains proprietary market intelligence. Never expose it publicly.**

- DB backups are stored as private GitHub Actions artifacts (30-day retention)
- No public GitHub Releases for DB files
- Dashboard runs locally only (no Streamlit Cloud deployment with public DB download)
- The `.db` file is committed to the repo — ensure the repo stays **private**

## Database

SQLite at `data/lombok_intel.db`. Key tables:
- **airbnb_listings** — master listing data (1,585 listings as of 2026-03-02)
- **booking_listings** — Booking.com listings (133 across 8 zones)
- **calendar_snapshots** — daily availability + price per listing per date (144,781 rows)
- **occupancy_events** — inferred bookings from calendar diffs (needs 2+ scrape runs)
- **price_history** — point-in-time price snapshots
- **zones** — 8 Lombok investment zones (GLI, SGG, NLB, MTR, KUT, TAA, SBK, SKT)
- **scrape_runs** — audit trail for every scrape execution

Key views: `v_adr_simple`, `v_forward_rates`, `v_occupancy_monthly`, `v_supply_by_zone`, `v_supply_growth`, `v_revpar_monthly`, `v_seasonality`, `v_scrape_health`, `v_all_listings`

**Warning:** `v_adr_by_zone_month` uses PERCENTILE() which doesn't exist in base SQLite. Use `v_adr_simple` instead.

## Hardening (implemented 2026-03-03)

### Automation Reliability
- Pinned dependencies in requirements.txt (exact versions, including pytest + setuptools)
- SQLite PRAGMA busy_timeout=30000 + synchronous=NORMAL in init_db.py
- GitHub Actions: concurrency guard, snapshot validation (2-hour window), log artifacts, permissions block
- API key fetch wrapped with tenacity retry (3 attempts, exponential backoff)
- RotatingFileHandler logging (5MB/file, 5 backups, DEBUG to file, WARNING to console)
- **Strict failure signaling**: main.py exits non-zero if all scrapers fail OR analysis/export fails
- **Analysis failures propagated**: `run_analysis()` returns bool, failures set exit code
- **Booking scraper exits non-zero** when run as `__main__` and status is "failed"
- **Workflow runs pytest** before scraping — regressions caught before data collection

### Scraper Robustness
- Booking.com: tenacity retry on page loads (3 attempts, exponential backoff)
- Booking.com: DB lock retry on snapshot inserts (5 attempts, exponential backoff)
- Airbnb: DB lock retry on upsert/insert operations
- Airbnb: 0-snapshot detection — marks run as "failed" if no listings found, "partial" if 0 snapshots
- playwright-stealth anti-detection, 6 weighted user agents, context rotation every 50 pages
- Image/CSS/font blocking via Playwright route interception
- **Playwright try/finally cleanup** in _stop_browser and _rotate_context (prevents orphaned processes)
- **Booking zone scan failure protection**: properties in failed zones are skipped (not written as "unavailable")
- **Booking undiscovered properties**: only writes snapshots for properties actually found in availability scan (no false "unavailable" for missing properties)

### Data Quality
- validate_coordinates() and validate_price() in utils.py (warn-only, never drops data)
- 11-sheet Excel export with try/finally on DB connection (includes Booking listings, calendar, combined zone summary)
- Dashboard RevPAR uses real occupancy from v_revpar_monthly, 60% placeholder only when empty
- Dashboard handles missing GeoJSON gracefully, PRAGMA failures don't crash on read-only mounts
- ADR calculator outputs p25, median, p75 percentiles + MoM growth rate
- **ADR dedup**: CTE with ROW_NUMBER keeps only latest run per (source, listing_id, snapshot_date)
- **Source in listing-night key**: prevents ID collisions between Airbnb and Booking
- **Occupancy rate capped at 1.0**: `.clip(upper=1.0)` prevents >100% occupancy from data anomalies
- **Occupancy events idempotent**: UNIQUE index + INSERT OR IGNORE prevents duplicate events on re-runs
- **NULL room count handling**: occupancy engine skips rows with NULL available_rooms (no false transitions from fillna(0))

### Notifications & Monitoring
- Telegram notify_telegram() helper (needs TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID secrets)
- GitHub Actions: success/failure Telegram alerts, weekly summary Issue (Mondays)
- DB backup as private workflow artifact (30-day retention)

### Testing & Maintenance
- 32 tests in tests/ (all passing)
- Database archival: src/db/archive.py moves snapshots >180 days to data/archive/
- **Archive uses chunked processing** (1000 rows at a time) to limit memory
- **Archive concurrent operation guard**: fcntl file lock prevents parallel archive runs
- **DB migrations**: init_db.py applies schema migrations once per process on first connection

## 8 Investment Zones

| Code | Name | Priority |
|------|------|----------|
| GLI | Gili Islands | 10 |
| SGG | Senggigi | 20 |
| NLB | North Lombok / Sire | 30 |
| MTR | Mataram | 40 |
| KUT | Kuta / Mandalika | 50 |
| TAA | Tanjung Aan / Gerupuk | 60 |
| SBK | Selong Belanak / West Surf | 70 |
| SKT | Sekotong | 80 |

Zone assignment uses bounding boxes in `src/config.py` with priority-based overlap resolution (lower number wins).

## Known Issues

### pyairbnb get_details() Cookies bug
`pyairbnb.get_details()` crashes with `'Cookies' object has no attribute 'isoformat'` on Python 3.14. Enrichment disabled. Search data provides sufficient fields.

### Occupancy engine needs 2+ runs
Returns 0 events until at least 2 calendar scrape runs exist to detect availability transitions. Daily cron handles this automatically.

## pyairbnb API Reference (verified signatures)

```python
pyairbnb.get_api_key(proxy_url=None)
pyairbnb.search_all(check_in, check_out, ne_lat, ne_long, sw_lat, sw_long, zoom_value, price_min, price_max, ...)
pyairbnb.get_calendar(api_key, room_id, proxy_url)
pyairbnb.get_details(room_id=int, currency='USD', proxy_url=None)  # BROKEN on Python 3.14
pyairbnb.get_price(room_id, check_in, check_out, adults=1, currency='USD', api_key=None, proxy_url=None)
```

**Search result field mapping** (pyairbnb returns non-standard keys):
- `room_id` (not `id`)
- `coordinates.latitude`, `coordinates.longitud` (typo in pyairbnb — not `longitude`)
- `price.unit.amount` for nightly price
- `rating.value` for overall rating
- `rating.reviewCount` (string, needs int conversion)

**Calendar response structure:**
```
[{month, year, days: [{calendarDate, available, price: {localPriceFormatted: null}}]}]
```
Note: `localPriceFormatted` is ALWAYS null. Price data must come from search results or `get_price()`.

## Tech Stack
- Python 3.14 (local venv at `.venv/`), target 3.12 for GitHub Actions
- pyairbnb, httpx[http2], curl-cffi, playwright, playwright-stealth, pandas, numpy, tenacity
- Playwright + headless Chromium for Booking.com (bypasses AWS WAF)
- Streamlit + Folium + Plotly for dashboard
- SQLite (WAL mode, busy_timeout=30s) for data store
- openpyxl for Excel export
- GitHub Actions for daily automation
- Telegram bot for push notifications

## GitHub Actions Secrets

| Secret | Purpose |
|--------|---------|
| `TELEGRAM_BOT_TOKEN` | Telegram bot token for notifications |
| `TELEGRAM_CHAT_ID` | Telegram chat ID for notifications |

`GITHUB_TOKEN` is provided automatically by GitHub Actions (used for artifact upload, issue creation, git push).
