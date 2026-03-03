"""Lombok Market Intelligence — Interactive Dashboard.

Run with: streamlit run src/dashboard/app.py
"""

import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path

import folium
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from branca.colormap import LinearColormap
from folium.plugins import HeatMap
from streamlit_folium import st_folium

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "lombok_intel.db"
GEOJSON_PATH = PROJECT_ROOT / "data" / "lombok_zones.geojson"

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Lombok Market Intelligence",
    page_icon="🏝",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

@st.cache_resource
def get_db():
    """Open a read-only database connection (cached across reruns)."""
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def query_df(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Run a SQL query and return a DataFrame."""
    conn = get_db()
    if conn is None:
        return pd.DataFrame()
    return pd.read_sql_query(sql, conn, params=params)


@st.cache_data(ttl=3600)
def load_geojson() -> dict:
    """Load zone GeoJSON from disk."""
    with open(GEOJSON_PATH) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Data loading functions
# ---------------------------------------------------------------------------

@st.cache_data(ttl=600)
def load_zones() -> pd.DataFrame:
    return query_df("SELECT * FROM zones ORDER BY zone_priority")


@st.cache_data(ttl=600)
def load_listings_summary() -> pd.DataFrame:
    return query_df("""
        SELECT zone_id, source, property_type,
               COUNT(*) as listing_count,
               AVG(nightly_price) as avg_price
        FROM (
            SELECT zone_id, 'airbnb' as source, property_type, nightly_price
            FROM airbnb_listings WHERE is_active = 1
            UNION ALL
            SELECT zone_id, 'booking' as source, property_type, NULL as nightly_price
            FROM booking_listings WHERE is_active = 1
        )
        GROUP BY zone_id, source, property_type
    """)


@st.cache_data(ttl=600)
def load_all_listings() -> pd.DataFrame:
    return query_df("""
        SELECT 'airbnb' as source, listing_id as id, name, property_type,
               latitude, longitude, zone_id, bedrooms, nightly_price,
               rating_overall as rating, review_count
        FROM airbnb_listings WHERE is_active = 1
        UNION ALL
        SELECT 'booking' as source, property_id as id, name, property_type,
               latitude, longitude, zone_id, NULL as bedrooms, NULL as nightly_price,
               review_score / 2.0 as rating, review_count
        FROM booking_listings WHERE is_active = 1
    """)


@st.cache_data(ttl=600)
def load_adr_data() -> pd.DataFrame:
    return query_df("""
        SELECT * FROM v_adr_simple
        WHERE zone_id IS NOT NULL
        ORDER BY zone_id, year_month
    """)


@st.cache_data(ttl=600)
def load_occupancy_data() -> pd.DataFrame:
    return query_df("""
        SELECT * FROM v_occupancy_monthly
        WHERE zone_id IS NOT NULL
        ORDER BY zone_id, year_month
    """)


@st.cache_data(ttl=600)
def load_supply_growth() -> pd.DataFrame:
    return query_df("SELECT * FROM v_supply_growth WHERE zone_id IS NOT NULL")


@st.cache_data(ttl=600)
def load_forward_rates() -> pd.DataFrame:
    return query_df("""
        SELECT * FROM v_forward_rates
        WHERE zone_id IS NOT NULL
        ORDER BY zone_id, snapshot_date
    """)


@st.cache_data(ttl=600)
def load_revpar_data() -> pd.DataFrame:
    return query_df("""
        SELECT * FROM v_revpar_monthly
        WHERE zone_id IS NOT NULL
        ORDER BY zone_id, year_month
    """)


@st.cache_data(ttl=600)
def load_scrape_health() -> pd.DataFrame:
    return query_df("SELECT * FROM v_scrape_health")


@st.cache_data(ttl=600)
def load_zone_adr_latest() -> pd.DataFrame:
    """Get the latest month's ADR per zone for map coloring."""
    return query_df("""
        SELECT zone_id, AVG(adr) as adr, SUM(priced_night_obs) as sample_size
        FROM v_adr_simple
        WHERE year_month = (SELECT MAX(year_month) FROM v_adr_simple)
        GROUP BY zone_id
    """)


# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------

def render_sidebar():
    st.sidebar.title("Lombok Market Intel")
    st.sidebar.markdown("---")

    # Data health indicator
    health = load_scrape_health()
    if health.empty:
        st.sidebar.warning("No scrape data yet. Run the scraper first.")
    else:
        for _, row in health.iterrows():
            src = row.get("source", "?")
            last = row.get("last_run_finished", "never")
            st.sidebar.caption(f"{src}: last run {last}")

    st.sidebar.markdown("---")

    # Filters
    zones_df = load_zones()
    zone_options = ["All Zones"] + [
        f"{r['zone_id']} - {r['name']}" for _, r in zones_df.iterrows()
    ] if not zones_df.empty else ["All Zones"]
    selected_zone = st.sidebar.selectbox("Zone", zone_options)

    source_filter = st.sidebar.radio("Data Source", ["Combined", "Airbnb", "Booking.com"])

    property_types = st.sidebar.multiselect(
        "Property Type",
        ["villa", "house", "apartment", "hotel", "guesthouse", "resort", "hostel"],
        default=[],
        help="Leave empty for all types",
    )

    bedroom_range = st.sidebar.slider("Bedrooms", 0, 10, (0, 10))

    price_range = st.sidebar.slider(
        "Price Range (USD/night)", 0, 1000, (0, 1000), step=25
    )

    return {
        "zone_id": selected_zone.split(" - ")[0] if selected_zone != "All Zones" else None,
        "source": source_filter.lower().replace(".", ""),
        "property_types": property_types,
        "bedrooms": bedroom_range,
        "price_range": price_range,
    }


# ---------------------------------------------------------------------------
# Map rendering
# ---------------------------------------------------------------------------

def render_map(filters: dict):
    """Render the interactive Folium map with zone choropleth and listing markers."""
    st.subheader("Lombok Investment Zones Map")

    geojson_data = load_geojson()
    zone_adr = load_zone_adr_latest()
    listings = load_all_listings()

    # Center on Lombok
    m = folium.Map(
        location=[-8.55, 116.25],
        zoom_start=10,
        tiles="OpenStreetMap",
    )

    # Color zones by ADR
    if not zone_adr.empty:
        adr_values = zone_adr["adr"].dropna()
        if len(adr_values) > 0:
            colormap = LinearColormap(
                colors=["#2ecc71", "#f1c40f", "#e74c3c"],
                vmin=adr_values.min(),
                vmax=adr_values.max(),
                caption="ADR (USD/night)",
            )

            # Add zone polygons with ADR coloring
            for feature in geojson_data["features"]:
                zone_id = feature["properties"]["zone_id"]
                zone_row = zone_adr[zone_adr["zone_id"] == zone_id]

                if not zone_row.empty:
                    adr_val = zone_row.iloc[0]["adr"]
                    sample = int(zone_row.iloc[0]["sample_size"])
                    color = colormap(adr_val)
                    tooltip = f"<b>{feature['properties']['name']}</b><br>ADR: ${adr_val:.0f}/night<br>Sample: {sample} obs"
                else:
                    color = "#cccccc"
                    tooltip = f"<b>{feature['properties']['name']}</b><br>No data yet"

                folium.GeoJson(
                    feature,
                    style_function=lambda x, c=color: {
                        "fillColor": c,
                        "color": "#333",
                        "weight": 2,
                        "fillOpacity": 0.5,
                    },
                    tooltip=folium.Tooltip(tooltip),
                ).add_to(m)

            colormap.add_to(m)
    else:
        # No ADR data yet — just show zone outlines
        for feature in geojson_data["features"]:
            fill = feature["properties"].get("fill_color", "#3388ff")
            folium.GeoJson(
                feature,
                style_function=lambda x, fc=fill: {
                    "fillColor": fc,
                    "color": "#333",
                    "weight": 2,
                    "fillOpacity": 0.35,
                },
                tooltip=folium.Tooltip(
                    f"<b>{feature['properties']['name']}</b><br>{feature['properties']['description']}"
                ),
            ).add_to(m)

    # Heatmap layer from listing locations
    if not listings.empty:
        heat_data = listings.dropna(subset=["latitude", "longitude"])
        if filters["zone_id"]:
            heat_data = heat_data[heat_data["zone_id"] == filters["zone_id"]]

        if not heat_data.empty:
            heat_points = heat_data[["latitude", "longitude"]].values.tolist()

            # Weight by price if available
            if "nightly_price" in heat_data.columns:
                prices = heat_data["nightly_price"].fillna(50).values
                prices_norm = (prices - prices.min()) / (prices.max() - prices.min() + 1)
                heat_points_weighted = [
                    [row[0], row[1], float(w)]
                    for row, w in zip(heat_data[["latitude", "longitude"]].values, prices_norm)
                ]
                HeatMap(heat_points_weighted, radius=15, blur=10, max_zoom=13).add_to(m)
            else:
                HeatMap(heat_points, radius=15, blur=10, max_zoom=13).add_to(m)

    map_data = st_folium(m, width=None, height=500, returned_objects=[])
    return map_data


# ---------------------------------------------------------------------------
# KPI cards
# ---------------------------------------------------------------------------

def render_kpi_cards(filters: dict):
    """Show top-level KPI summary cards."""
    listings = load_all_listings()
    adr = load_adr_data()
    occupancy = load_occupancy_data()

    if filters["zone_id"]:
        listings = listings[listings["zone_id"] == filters["zone_id"]]
        adr = adr[adr["zone_id"] == filters["zone_id"]]
        occupancy = occupancy[occupancy["zone_id"] == filters["zone_id"]]

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric("Active Listings", len(listings) if not listings.empty else 0)

    with col2:
        if not adr.empty:
            latest_adr = adr.iloc[-1]["adr"] if "adr" in adr.columns else 0
            st.metric("ADR", f"${latest_adr:.0f}" if latest_adr else "N/A")
        else:
            st.metric("ADR", "N/A")

    with col3:
        if not occupancy.empty:
            latest_occ = occupancy.iloc[-1]["booked_nights"] if "booked_nights" in occupancy.columns else 0
            st.metric("Booked Nights (Month)", int(latest_occ) if latest_occ else 0)
        else:
            st.metric("Booked Nights", "N/A")

    with col4:
        if not adr.empty:
            try:
                a = adr.iloc[-1]["adr"] or 0
                # Try real occupancy from v_revpar_monthly
                revpar_df = load_revpar_data()
                if filters["zone_id"]:
                    revpar_df = revpar_df[revpar_df["zone_id"] == filters["zone_id"]]
                if not revpar_df.empty and "occupancy_rate" in revpar_df.columns:
                    occ_rate = revpar_df.iloc[-1].get("occupancy_rate")
                    if occ_rate is not None and occ_rate > 0:
                        st.metric("RevPAR", f"${a * occ_rate:.0f}")
                    else:
                        st.metric("Est. RevPAR", f"${a * 0.6:.0f}",
                                  help="Using 60% placeholder (no occupancy data yet)")
                else:
                    st.metric("Est. RevPAR", f"${a * 0.6:.0f}",
                              help="Using 60% placeholder (no occupancy data yet)")
            except (KeyError, TypeError):
                st.metric("Est. RevPAR", "N/A")
        else:
            st.metric("Est. RevPAR", "N/A")

    with col5:
        sources = listings["source"].nunique() if not listings.empty else 0
        st.metric("Data Sources", sources)


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------

def render_adr_trends(filters: dict):
    """ADR trend chart by zone over time."""
    st.subheader("ADR Trends by Zone")
    adr = load_adr_data()
    if adr.empty:
        st.info("No ADR data available yet. Run the scraper to collect pricing data.")
        return

    if filters["zone_id"]:
        adr = adr[adr["zone_id"] == filters["zone_id"]]

    if filters["source"] not in ("combined",):
        source_map = {"airbnb": "airbnb", "bookingcom": "booking"}
        s = source_map.get(filters["source"])
        if s:
            adr = adr[adr["source"] == s]

    if adr.empty:
        st.info("No data for selected filters.")
        return

    fig = px.line(
        adr,
        x="year_month",
        y="adr",
        color="zone_id",
        markers=True,
        labels={"year_month": "Month", "adr": "ADR (USD)", "zone_id": "Zone"},
    )
    fig.update_layout(
        height=400,
        margin=dict(l=0, r=0, t=30, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig, use_container_width=True)


def render_occupancy_trends(filters: dict):
    """Occupancy trend chart."""
    st.subheader("Booking Activity by Zone")
    occ = load_occupancy_data()
    if occ.empty:
        st.info("No occupancy data yet. This requires ~30 days of daily calendar scraping.")
        return

    if filters["zone_id"]:
        occ = occ[occ["zone_id"] == filters["zone_id"]]

    fig = px.bar(
        occ,
        x="year_month",
        y="booked_nights",
        color="zone_id",
        barmode="group",
        labels={"year_month": "Month", "booked_nights": "Booked Nights", "zone_id": "Zone"},
    )
    fig.update_layout(height=400, margin=dict(l=0, r=0, t=30, b=0))
    st.plotly_chart(fig, use_container_width=True)


def render_supply_growth(filters: dict):
    """Supply growth over time."""
    st.subheader("Supply Growth (Cumulative Listings)")
    supply = load_supply_growth()
    if supply.empty:
        st.info("No supply data yet.")
        return

    if filters["zone_id"]:
        supply = supply[supply["zone_id"] == filters["zone_id"]]

    fig = px.area(
        supply,
        x="first_seen_month",
        y="cumulative_listings",
        color="zone_id",
        labels={
            "first_seen_month": "Month",
            "cumulative_listings": "Total Listings",
            "zone_id": "Zone",
        },
    )
    fig.update_layout(height=400, margin=dict(l=0, r=0, t=30, b=0))
    st.plotly_chart(fig, use_container_width=True)


def render_forward_curve(filters: dict):
    """Forward rate curve for upcoming dates."""
    st.subheader("Forward Rate Curve")
    fwd = load_forward_rates()
    if fwd.empty:
        st.info("No forward rate data yet.")
        return

    if filters["zone_id"]:
        fwd = fwd[fwd["zone_id"] == filters["zone_id"]]

    fig = px.line(
        fwd,
        x="snapshot_date",
        y="avg_price",
        color="zone_id",
        labels={
            "snapshot_date": "Future Date",
            "avg_price": "Avg Nightly Rate (USD)",
            "zone_id": "Zone",
        },
    )
    fig.update_layout(height=400, margin=dict(l=0, r=0, t=30, b=0))
    st.plotly_chart(fig, use_container_width=True)


def render_zone_comparison(filters: dict):
    """Side-by-side zone comparison table."""
    st.subheader("Zone Comparison")
    listings = load_all_listings()
    if listings.empty:
        st.info("No listing data yet.")
        return

    zone_stats = (
        listings.groupby("zone_id")
        .agg(
            count=("id", "count"),
            avg_price=("nightly_price", "mean"),
            avg_rating=("rating", "mean"),
            avg_reviews=("review_count", "mean"),
        )
        .reset_index()
    )

    zones_df = load_zones()
    if not zones_df.empty:
        zone_names = zones_df.set_index("zone_id")["name"].to_dict()
        zone_stats["zone_name"] = zone_stats["zone_id"].map(zone_names)
    else:
        zone_stats["zone_name"] = zone_stats["zone_id"]

    zone_stats["avg_price"] = zone_stats["avg_price"].round(0)
    zone_stats["avg_rating"] = zone_stats["avg_rating"].round(2)
    zone_stats["avg_reviews"] = zone_stats["avg_reviews"].round(0)

    display_cols = ["zone_id", "zone_name", "count", "avg_price", "avg_rating", "avg_reviews"]
    display_cols = [c for c in display_cols if c in zone_stats.columns]

    st.dataframe(
        zone_stats[display_cols].rename(columns={
            "zone_id": "Zone",
            "zone_name": "Name",
            "count": "Listings",
            "avg_price": "Avg Price ($)",
            "avg_rating": "Avg Rating",
            "avg_reviews": "Avg Reviews",
        }),
        use_container_width=True,
        hide_index=True,
    )


def render_data_export(filters: dict):
    """CSV export section."""
    st.subheader("Export Data")
    col1, col2, col3 = st.columns(3)

    with col1:
        listings = load_all_listings()
        if not listings.empty:
            if filters["zone_id"]:
                listings = listings[listings["zone_id"] == filters["zone_id"]]
            csv = listings.to_csv(index=False)
            st.download_button(
                "Download Listings CSV",
                csv,
                "lombok_listings.csv",
                "text/csv",
            )

    with col2:
        adr = load_adr_data()
        if not adr.empty:
            csv = adr.to_csv(index=False)
            st.download_button(
                "Download ADR CSV",
                csv,
                "lombok_adr.csv",
                "text/csv",
            )

    with col3:
        occ = load_occupancy_data()
        if not occ.empty:
            csv = occ.to_csv(index=False)
            st.download_button(
                "Download Occupancy CSV",
                csv,
                "lombok_occupancy.csv",
                "text/csv",
            )


# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------

def main():
    filters = render_sidebar()

    st.title("Lombok Market Intelligence")
    st.caption("Proprietary STR market data for real estate investment analysis")

    # Check if database exists
    conn = get_db()
    if conn is None:
        st.warning(
            "Database not found. Run `python -m src.db.init_db` to initialize, "
            "then run the scrapers to collect data."
        )
        # Still show the map with zone outlines
        render_map(filters)
        return

    # KPI cards
    render_kpi_cards(filters)

    st.markdown("---")

    # Map
    render_map(filters)

    st.markdown("---")

    # Charts in tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "ADR Trends",
        "Booking Activity",
        "Supply Growth",
        "Forward Rates",
        "Zone Comparison",
    ])

    with tab1:
        render_adr_trends(filters)
    with tab2:
        render_occupancy_trends(filters)
    with tab3:
        render_supply_growth(filters)
    with tab4:
        render_forward_curve(filters)
    with tab5:
        render_zone_comparison(filters)

    st.markdown("---")

    # Export
    render_data_export(filters)

    # Footer
    st.markdown("---")
    st.caption(
        "Lombok Market Intelligence v1.0 | "
        "Data inferred from public OTA listings | "
        "Occupancy estimates undercount actual occupancy by ~20-30%"
    )


if __name__ == "__main__":
    main()
