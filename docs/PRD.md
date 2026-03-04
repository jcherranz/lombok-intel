# Product Requirements Document (PRD)
## Product: Lombok Market Intelligence
## Version: 1.0 (MVP)
## Date: March 2, 2026
## Owner: Private project (proprietary data)

> **Implementation Note (2026-03-04):** This PRD was written before implementation.
> Key divergences from the actual build:
> - **Data store**: SQLite (not DuckDB) — simpler, sufficient for the workload
> - **Booking.com scraper**: Playwright + stealth HTML scraping (not httpx/GraphQL — blocked by AWS WAF)
> - **Python version**: 3.12 in CI, 3.14 locally (not 3.11+)
> - **Dashboard hosting**: Local only (not Streamlit Cloud — DB is proprietary)
> - **Repo**: Private, not open-source (proprietary market data)
> - **Zone assignment**: Bounding boxes with priority overlap (not point-in-polygon)
>
> See `CLAUDE.md` for the authoritative technical reference.

## 1. Executive Summary
Lombok Market Intelligence is a proprietary market data platform that continuously collects short-term rental data in Lombok, Indonesia from Airbnb and Booking.com, then transforms that raw supply and pricing data into decision-ready investment metrics.

The product addresses a specific gap in Lombok real estate underwriting: investors can estimate land and construction costs, but usually lack reliable, longitudinal data for occupancy and achievable room rates. By running daily data collection and occupancy inference pipelines, the system produces a proprietary local database of ADR, occupancy, RevPAR, seasonality, and supply trends across 8 custom investment zones.

The product is designed as a "set it and forget it" workflow for non-technical users:
- Automated data scraping and processing via GitHub Actions (daily)
- Persistent local analytical store in SQLite or DuckDB
- Interactive Streamlit dashboard with maps, filters, and trend charts
- Zero paid APIs or infrastructure dependencies

Primary value proposition:
- Build and own a proprietary data moat for Lombok STR investment analysis
- Reduce underwriting uncertainty by replacing anecdotal assumptions with measured market signals
- Support ongoing investment monitoring, not one-time feasibility studies

## 2. Problem Statement
Real estate investors in Lombok face a data scarcity problem:
- Public market reports are infrequent, expensive, and too aggregated for micro-location decisions.
- Platform-native listings show point-in-time prices but do not directly expose historical occupancy.
- Brokers and local operators often provide anecdotal occupancy and ADR figures that are hard to verify.
- Financial models (ROI, payback, DSCR, cash flow) are highly sensitive to occupancy and rate assumptions.

As a result, investment decisions are often made with weak data quality, causing:
- Over-optimistic revenue projections
- Mispricing of land or assets
- Poor zone selection (buying in low-performing submarkets)
- Inability to track market shifts after acquisition

Lombok Market Intelligence solves this by continuously collecting supply, availability, and rate signals from major OTA platforms and converting them into repeatable, zone-level metrics that can feed investment models.

## 3. Goals & Success Metrics
### 3.1 Product Goals
1. Create a continuously updated, queryable database of Lombok STR listings and rates.
2. Estimate occupancy rates using transparent, defensible inference methods for Airbnb and Booking.com.
3. Deliver a simple dashboard usable by non-technical real estate decision makers.
4. Keep total operating cost at zero (free-tier only).
5. Provide exportable inputs for financial modeling (ADR, occupancy, RevPAR, seasonality, forward curves).

### 3.2 Success Metrics (MVP)
- Coverage:
  - >= 85% of active Airbnb listings in target zones discovered and tracked
  - >= 70% of relevant Booking.com properties in target zones tracked
- Freshness:
  - Daily pipeline success rate >= 95% over trailing 30 days
  - Data latency <= 24 hours from scrape to dashboard availability
- Data utility:
  - Daily ADR and occupancy values available for each zone
  - RevPAR and seasonality index available per zone and property type segment
- User outcome:
  - A non-technical user can answer within 5 minutes:
    - "What is current ADR and occupancy in my target zone?"
    - "How has this changed in the last 90 days?"
    - "How does one zone compare with alternatives?"
- Reliability:
  - End-to-end pipeline recovers automatically from transient failures (retry + alert)

### 3.3 North Star Metric
- Number of days with complete, trusted zone-level KPI snapshots (ADR, occupancy, RevPAR) across all 8 zones.

## 4. User Personas
### Persona A: Primary User - Real Estate Developer (Non-Technical)
- Profile: Local/international investor evaluating villa/hotel developments in Lombok.
- Skills: Strong finance and construction understanding; low coding comfort.
- Goals:
  - Validate demand assumptions before land purchase or build decision.
  - Compare micro-markets (Kuta vs Selong Belanak vs Gili, etc.).
  - Plug market assumptions into ROI/cash flow models.
- Pain points:
  - No trusted occupancy benchmark by submarket.
  - Data scattered across platforms; manual collection is slow.
  - Hard to distinguish seasonal dip from structural weakness.

### Persona B: Secondary User - Analyst / Acquisition Associate
- Profile: Supports underwriting, produces investment memos.
- Skills: Comfortable with spreadsheets, limited Python.
- Goals:
  - Build quarterly and monthly market updates.
  - Segment by property type, bedrooms, and price tier.
  - Download clean time series for model input.

### Persona C: Maintainer - Technical Contributor
- Profile: Open-source developer maintaining scrapers, pipeline, and dashboard.
- Goals:
  - Keep system stable despite upstream endpoint changes.
  - Improve inference quality and observability over time.

## 5. Data Sources & Collection Strategy
### 5.1 Source A: Airbnb via `pyairbnb`
- Access method: `pyairbnb` library against Airbnb GraphQL v3 API.
- Extracted fields:
  - Listing metadata: listing id, title, property type, bedrooms, bathrooms, max guests
  - Location: latitude, longitude, neighborhood descriptors
  - Pricing: nightly rate, fees if available
  - Availability: 365-day calendar status by date
  - Quality and trust signals: ratings, review count, host details
  - Amenities and other listing attributes
- Collection cadence:
  - Daily full-zone search pull for listing discovery/updates
  - Daily calendar polling for tracked listing ids (for occupancy inference)

### 5.2 Source B: Booking.com via GraphQL (`/dml/graphql`)
- Access method: direct `httpx` calls to internal endpoint.
- Extracted fields:
  - Property metadata: name, type, location, review score, amenities
  - Room-level rate and availability signals
  - GPS coordinates
- Collection cadence:
  - Daily zone-level property pull
  - Daily room-availability depth probing (1..N room requests)

### 5.3 Collection Strategy
- Zone-first harvesting:
  - Use 8 fixed custom zones as primary query partitions.
- Incremental persistence:
  - Append raw snapshots daily for reproducibility and backtesting.
- Entity normalization:
  - Assign canonical internal ids and source-specific external ids.
- Idempotency:
  - Unique keys per source/date/entity to avoid duplicate rows.
- Resilience:
  - Retries with exponential backoff for rate limits and transient failures.
- Monitoring:
  - Log row counts, success/failure by source and zone, and anomaly warnings.

### 5.4 Compliance and Usage Notes
- The project must include clear disclaimers that data is inferred and for research use.
- Contributors must review platform terms and applicable legal constraints in their jurisdiction before operating scrapers.
- Robots and anti-bot protections can change; architecture must tolerate endpoint and schema volatility.

## 6. Data Model (high level)
### 6.1 Core Entities
1. `zones`
- `zone_id` (PK): KUT, SBK, TAA, SGG, GLI, MTR, SKT, NLB
- `zone_name`, `center_lat`, `center_lng`, `character`, `geojson_geometry`

2. `listings`
- Internal canonical listing record across platforms
- `listing_uid` (PK), `source` (`airbnb` or `booking`), `source_listing_id`
- Property attributes: type, bedrooms, bathrooms, capacity, amenities hash
- Host/operator fields where available
- Geospatial fields: lat/lng, zone_id

3. `daily_listing_snapshot`
- Point-in-time listing facts per scrape date
- `snapshot_date`, `listing_uid`, price signals, review counts, rating, active status

4. `calendar_daily`
- Date-level availability per listing for stay dates
- `observed_date` (when scraped), `stay_date`, `listing_uid`, `status` (available/blocked/unavailable)

5. `booking_room_probe`
- Availability depth probes for Booking.com
- `observed_date`, `listing_uid`, `requested_rooms`, `is_available`, `quoted_price`

6. `occupancy_signals`
- Inference events and confidence
- `listing_uid`, `stay_date`, `inferred_booked` (bool), `confidence`, `method`, `source`

7. `daily_metrics_zone`
- Final KPI table powering dashboard
- `date`, `zone_id`, `source`, `property_type_segment`, `bedroom_segment`
- `adr`, `occupancy_rate`, `revpar`, `listing_count`, `new_listing_count`, `seasonality_index`

8. `forward_rate_curve`
- `observed_date`, `zone_id`, `days_out` (1,7,14,30,60,90), `median_rate`, `p25`, `p75`

### 6.2 Storage Choice
- MVP default: DuckDB (analytics-friendly, fast local OLAP queries).
- Alternative: SQLite (simpler ubiquity, lower analytical performance).
- Decision principle:
  - Use DuckDB for dashboard speed and time-series aggregation.
  - Keep schema SQL portable where practical.

### 6.3 Data Quality Fields
Each derived metric row should include:
- `sample_size`
- `coverage_ratio`
- `method_version`
- `quality_flag` (good/warn/low_confidence)

## 7. Occupancy Inference Methodology
### 7.1 Airbnb Occupancy Inference
Method: daily calendar transition tracking.

Logic:
1. Poll listing calendars daily for 365-day horizon.
2. Detect transitions for each listing-stay_date pair:
- `available -> blocked/unavailable`: probable booking event
- `blocked -> available`: cancellation or owner release
3. Classify blocks as likely booking vs likely owner block using heuristics:
- Transition persistence (short-lived blocks less reliable)
- Pattern shape (large contiguous owner blocks vs fragmented demand-like pickup)
- Minimum-stay compatibility
- Review correlation lag (new review after stay window supports booking hypothesis)
- Historical host behavior baseline
4. Generate inferred occupancy events with confidence scores.

Output:
- Daily inferred occupancy percentage by listing and zone.

### 7.2 Booking.com Occupancy Inference
Method: room availability depth probing ("disappearing rooms" approximation).

Logic:
1. Query property availability requesting 1,2,3,...,N rooms.
2. Identify highest room count still available for a target date.
3. Estimate total room stock using far-future low-demand dates as reference baseline.
4. Occupancy estimate per date:
- `estimated_occupied = max_stock - currently_available_stock`
- `occupancy_rate = estimated_occupied / max_stock`

### 7.3 Bias and Calibration
Known limitation:
- OTA-derived occupancy underestimates true occupancy by approximately 20-30% due to off-platform bookings (direct, WhatsApp, walk-ins, long-stay contracts).

Calibration strategy (MVP):
- Keep both raw occupancy and calibrated occupancy views.
- Calibrated mode applies configurable correction factor range (for scenario analysis).
- Display explicit disclaimer in dashboard and docs.

### 7.4 Validation Approach
- Internal consistency checks:
  - Occupancy bounded [0,1]
  - RevPAR consistency (`RevPAR ~= ADR * Occupancy`)
- Temporal plausibility checks:
  - Spike detection and outlier flags
- Cross-signal checks:
  - Review velocity vs occupancy trend direction

## 8. Dashboard & Visualization Requirements
### 8.1 UX Principles
- Designed for non-technical decision makers.
- Clear defaults, minimal configuration.
- Every chart answer must map to an investment question.

### 8.2 Core Screens (Streamlit)
1. **Market Overview**
- KPI cards: ADR, Occupancy, RevPAR, Active Listings, Supply Growth (30d)
- Source toggle: Airbnb / Booking.com / Combined

2. **Interactive Map View**
- Folium-based map with OpenStreetMap tiles
- Zone choropleth colored by selected metric (ADR or occupancy)
- Listing density heatmap with optional price intensity layer
- Click zone to open detailed metric panel

3. **Zone Detail View**
- Median ADR, occupancy, RevPAR
- Distribution charts by property type and bedroom segment
- Seasonality chart (monthly index)
- Forward rate curve (1/7/14/30/60/90 days out)

4. **Trends & Comparison View**
- Time series for ADR and occupancy by zone
- Multi-zone comparison mode
- Supply growth (new listings over time)

### 8.3 Filters (Sidebar)
- Date range
- Zone(s)
- Source platform
- Property type
- Bedrooms
- Price range
- Confidence/quality threshold

### 8.4 Exports
- CSV export for filtered KPI tables and time series.
- Optional ready-to-use underwriting extract (monthly ADR/occupancy assumptions).

### 8.5 Update Behavior
- Data refreshes automatically after daily pipeline run.
- Dashboard shows "last updated" timestamp and pipeline status.

## 9. Technical Architecture
### 9.1 Stack
- Language: Python 3.11+
- Scrapers: `pyairbnb`, `httpx`
- Data store: DuckDB (preferred) or SQLite
- Pipeline: Python modules in `src/pipeline`
- Dashboard: Streamlit + Folium
- Orchestration: GitHub Actions cron
- Hosting: Streamlit Community Cloud
- Maps: OpenStreetMap tiles

### 9.2 Repository Structure
```text
lombok-intel/
├── docs/
│   └── PRD.md
├── src/
│   ├── scrapers/
│   │   ├── airbnb_scraper.py
│   │   └── booking_scraper.py
│   ├── pipeline/
│   │   ├── occupancy_engine.py
│   │   └── adr_calculator.py
│   ├── db/
│   │   └── schema.sql
│   └── dashboard/
│       └── app.py
├── data/
│   └── lombok_zones.geojson
├── .github/
│   └── workflows/
│       └── scrape.yml
├── requirements.txt
└── README.md
```

### 9.3 Daily Pipeline Flow
1. Trigger: GitHub Actions cron (daily).
2. Scrape Airbnb listings + calendars by zone.
3. Scrape Booking.com properties + room probes by zone.
4. Persist raw snapshots.
5. Run transformation and inference jobs:
- ADR calculation
- Occupancy inference
- RevPAR and seasonality
- Forward curve computation
6. Write/overwrite aggregate KPI tables.
7. Publish artifacts and logs; refresh dashboard data.

### 9.4 Non-Functional Requirements
- Reliability: graceful retries and partial-failure tolerance
- Performance: daily run completes within GitHub Actions free-tier limits
- Observability: structured logs + run summary metrics
- Reproducibility: deterministic transforms with versioned inference logic
- Security: no hardcoded secrets, minimal token usage

## 10. Geographic Zone Definitions
The platform operates on 8 custom Lombok investment zones:

| Zone ID | Name | Approx Center (lat,lng) | Character |
|---|---|---|---|
| KUT | Kuta / Mandalika | -8.90, 116.30 | SEZ, MotoGP, high managed villa share |
| SBK | Selong Belanak / West Surf | -8.92, 116.22 | Luxury villas, premium surf demand |
| TAA | Tanjung Aan / Gerupuk | -8.91, 116.35 | East surf coast, emerging supply |
| SGG | Senggigi | -8.49, 116.05 | Traditional tourist strip |
| GLI | Gili Islands | -8.35, 116.06 | Iconic micro-market, island demand |
| MTR | Mataram | -8.58, 116.12 | Capital city, business and domestic travel |
| SKT | Sekotong | -8.74, 115.98 | Early-stage market, low land cost |
| NLB | North Lombok / Sire | -8.35, 116.12 | Luxury resorts, marina corridor |

Implementation notes:
- Geo boundaries stored in `data/lombok_zones.geojson`.
- Listing-to-zone assignment uses point-in-polygon first, then nearest-zone fallback where needed.

## 11. MVP Scope vs Future Enhancements
### 11.1 MVP Scope (Must-have)
- Daily scraping from Airbnb and Booking.com for 8 Lombok zones
- Persistent raw + transformed database
- ADR, occupancy (inferred), RevPAR calculations
- Seasonality index and supply growth basics
- Forward rate curve (1,7,14,30,60,90 days out)
- Streamlit dashboard with map, filters, trend charts
- GitHub Actions automation and basic run monitoring
- Export to CSV

### 11.2 Post-MVP Enhancements (Should/Could)
- Better occupancy calibration with local operator ground truth samples
- Property-level comp set builder (custom comparable groups)
- Alerting (price drop, occupancy anomaly, sudden supply surge)
- Forecasting models for ADR/occupancy (short horizon)
- Additional channels (Agoda, Expedia, direct website scraping where legal)
- API layer for external model integration
- Scenario simulator for underwriting (base/upside/downside curves)
- User auth and shared dashboards for team workflows

## 12. Risks & Mitigations
### 12.1 Data Access Risk
Risk:
- Upstream API schema changes, anti-bot defenses, or endpoint blocking.
Mitigation:
- Modular scraper adapters, schema validation, retry logic, and rapid patch workflow.

### 12.2 Legal/Compliance Risk
Risk:
- Terms-of-service violations or jurisdictional restrictions.
Mitigation:
- Clear legal disclaimer, contributor guidance, conservative request rates, and configurable source disable switches.

### 12.3 Inference Accuracy Risk
Risk:
- Occupancy estimates are imperfect and structurally biased low.
Mitigation:
- Confidence scoring, calibrated and raw modes, transparent methodology documentation.

### 12.4 Operational Reliability Risk
Risk:
- Daily jobs fail silently, creating stale dashboard decisions.
Mitigation:
- Pipeline run status checks, failure notifications, visible "last updated" timestamp.

### 12.5 Data Quality Drift
Risk:
- Geographic misclassification, duplicate listings, outlier rates.
Mitigation:
- Zone polygon validation, deduping keys, anomaly rules, periodic manual QA audits.

### 12.6 Free-tier Resource Limits
Risk:
- GitHub Actions minute limits and Streamlit resource constraints.
Mitigation:
- Optimize scrape scope, incremental updates, lightweight transforms, and fallback local execution option.

## 13. Timeline (phases)
### Phase 0: Setup and Foundation (Week 1)
- Initialize repo structure and environment
- Implement database schema and zone geojson
- Create scraper skeletons and shared utilities
- Define logging and run metadata

Deliverable:
- Running local pipeline scaffold with stub outputs.

### Phase 1: Data Ingestion MVP (Weeks 2-3)
- Implement Airbnb listing + calendar ingestion
- Implement Booking property + room-probe ingestion
- Persist raw snapshots with idempotent keys
- Add daily GitHub Actions schedule

Deliverable:
- Daily raw data ingestion for all 8 zones.

### Phase 2: Metrics and Inference Engine (Weeks 4-5)
- Implement ADR calculator
- Implement Airbnb and Booking occupancy inference logic
- Compute zone-level occupancy, RevPAR, supply growth, forward curves
- Add quality flags and validation checks

Deliverable:
- Queryable metrics tables suitable for dashboard.

### Phase 3: Dashboard MVP (Weeks 6-7)
- Build Streamlit UI with KPI cards, filters, map, trend charts
- Add zone drill-down and export functionality
- Show freshness and data quality indicators

Deliverable:
- End-user dashboard hosted on Streamlit Community Cloud.

### Phase 4: Hardening and Documentation (Week 8)
- Improve error handling and observability
- Add setup docs and methodology notes
- Add smoke tests and sanity checks
- Prepare open-source release notes

Deliverable:
- Stable MVP release tagged `v1.0.0`.

---

## Acceptance Criteria (MVP Release)
- Daily automated run completes and updates metrics without manual intervention.
- Dashboard exposes ADR, occupancy, RevPAR, seasonality, and supply growth for each of the 8 zones.
- User can filter and compare zones over time and export data for financial modeling.
- Inference assumptions and known biases are documented clearly in repo docs.
- System operates without paid APIs or paid hosting dependencies.
