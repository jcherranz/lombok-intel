# Lombok Market Intelligence

Short-term rental market intelligence for Lombok, Indonesia. Scrapes Airbnb (and optionally Booking.com), infers occupancy from calendar diffs, computes ADR/RevPAR, and serves an interactive Streamlit dashboard with Folium maps.

## Quick Reference

```bash
source .venv/bin/activate          # Always activate venv first
python main.py                     # Full pipeline: scrape + analyze + excel export
python main.py --scrape            # Scrapers only
python main.py --analyze           # Analysis only
python main.py --dashboard         # Launch Streamlit dashboard
python -m src.export_excel         # Excel export only
python -m src.scrapers.airbnb_scraper  # Airbnb scraper standalone
```

## Project Structure

```
main.py                          # CLI orchestrator (--scrape, --analyze, --dashboard)
src/
  config.py                      # Zone bounding boxes, scrape delays, shared constants
  utils.py                       # setup_logger, assign_zone, rate_limit, now_iso
  export_excel.py                # DB → data/lombok_intel.xlsx (7 sheets)
  db/
    schema.sql                   # 12 tables, 19 indexes, 10 views
    init_db.py                   # init_database(), get_connection()
  scrapers/
    airbnb_scraper.py            # AirbnbScraper — pyairbnb GraphQL v3
    booking_scraper.py           # BookingScraper — httpx/GraphQL (NOT YET TESTED)
  pipeline/
    occupancy_engine.py          # OccupancyEngine — calendar diff → occupancy_events
    adr_calculator.py            # ADRCalculator — ADR, RevPAR, forward curves, seasonality
  dashboard/
    app.py                       # Streamlit + Folium interactive map dashboard
data/
  lombok_intel.db                # SQLite database (the persistent data store)
  lombok_intel.xlsx              # Excel export (regenerated each run)
  lombok_zones.geojson           # 8 zone polygons for map rendering
docs/
  PRD.md                         # Full product requirements document
.github/workflows/
  scrape.yml                     # Daily GitHub Actions pipeline (needs fixes, see below)
```

## Database

SQLite at `data/lombok_intel.db`. Key tables:
- **airbnb_listings** — master listing data (1,585 listings as of 2026-03-02)
- **booking_listings** — Booking.com listings (empty — scraper not yet tested)
- **calendar_snapshots** — daily availability + price per listing per date (only 5 listings scraped, prices are NULL)
- **occupancy_events** — inferred bookings from calendar diffs (empty — needs 2+ scrape runs)
- **price_history** — point-in-time price snapshots (empty)
- **zones** — 8 Lombok investment zones (GLI, SGG, NLB, MTR, KUT, TAA, SBK, SKT)
- **scrape_runs** — audit trail for every scrape execution

Key views: `v_adr_simple`, `v_forward_rates`, `v_occupancy_monthly`, `v_supply_by_zone`, `v_supply_growth`, `v_revpar_monthly`, `v_seasonality`, `v_scrape_health`, `v_all_listings`

**Warning:** `v_adr_by_zone_month` uses PERCENTILE() which doesn't exist in base SQLite. Use `v_adr_simple` instead.

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

## Known Bugs & Blockers (as of 2026-03-02)

### CRITICAL: Calendar prices are always NULL
`pyairbnb.get_calendar()` returns `localPriceFormatted: null` for all days. The `v_adr_simple` and `v_forward_rates` views filter on `cs.price IS NOT NULL`, so they return 0 rows.
**Planned fix:** Fall back to `nightly_price` from `airbnb_listings` table when calendar price is null. See execution plan.

### pyairbnb get_details() Cookies bug
`pyairbnb.get_details()` crashes with `'Cookies' object has no attribute 'isoformat'` on Python 3.14. The enrichment step (`_enrich_new_listings`) is disabled. Search data provides sufficient fields for MVP.

### Scrape run #1 stuck in "running"
The first test run was interrupted but never finalized. Fix: `UPDATE scrape_runs SET status='partial', finished_at=datetime('now') WHERE status='running'`

### .gitignore blocks database commits
`.gitignore` has `*.db` which prevents GitHub Actions from committing the database. Fix: add `!data/lombok_intel.db`.

### GitHub Actions workflow needs fixes
- timeout-minutes too short (45 → 300)
- No run_type support (should use calendar_only daily, full weekly)
- Python 3.11 → 3.12 (3.14 has pyairbnb Cookies bug)

### Git push auth
Token not embedded in remote URL. Need `gh auth setup-git` or fresh token.

### No resume logic in calendar scraper
If interrupted, re-scrapes all listings from scratch. Planned fix: skip listings that already have snapshots for current run_id.

## Execution Plan

Full plan at `.claude/plans/velvet-watching-pizza.md`. Summary:
1. **Phase 0:** Fix calendar price fallback, add resume logic, clean stale run, fix .gitignore, fix workflow, push
2. **Phase 1:** Launch calendar scrape (background, ~3-4 hrs), launch dashboard, test Booking.com
3. **Phase 2:** Run ADR calculator + occupancy engine after scrape completes
4. **Phase 3:** Verify end-to-end, commit data, push

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
- pyairbnb, httpx[http2], curl-cffi, pandas, numpy
- Streamlit + Folium + Plotly for dashboard
- SQLite (WAL mode) for data store
- openpyxl for Excel export
- GitHub Actions for daily automation
