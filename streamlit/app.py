"""
Airline Analytics Dashboard — Overview
=======================================
Home page showing KPIs and a daily traffic summary.
Sidebar filters (date range, airports) are shared across all pages.
"""

import streamlit as st

from utils.snowflake_conn import run_query, format_airport_list
from utils import queries, charts

# ── Page config ─────────────────────────────────────────────────────
st.set_page_config(
    page_title="Airline Analytics",
    page_icon=":airplane:",
    layout="wide",
)

# ── Sidebar: shared filters ─────────────────────────────────────────
st.sidebar.title("Filters")

# Fetch available airports and date boundaries (cached)
airports_df = run_query(queries.AVAILABLE_AIRPORTS)
date_bounds = run_query(queries.DATE_RANGE)

min_date = date_bounds["min_date"].iloc[0]
max_date = date_bounds["max_date"].iloc[0]

date_range = st.sidebar.date_input(
    "Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

# Handle single-date selection gracefully
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range[0] if isinstance(date_range, tuple) else date_range

selected_airports = st.sidebar.multiselect(
    "Airports",
    options=airports_df["airport_icao"].tolist(),
    default=airports_df["airport_icao"].tolist(),
    format_func=lambda x: (
        f"{x} — {airports_df[airports_df['airport_icao'] == x]['city'].iloc[0]}"
    ),
)

# Store in session_state so pages can access them
st.session_state["start_date"] = start_date
st.session_state["end_date"] = end_date
st.session_state["selected_airports"] = selected_airports
st.session_state["airports_df"] = airports_df

# ── Main content ────────────────────────────────────────────────────
st.title("Airline Analytics Dashboard")
st.caption(
    "Real-time flight data from the OpenSky Network, "
    "transformed through a Bronze → Silver → Gold medallion architecture using dbt."
)

if not selected_airports:
    st.warning("Select at least one airport in the sidebar.")
    st.stop()

# ── KPI cards ───────────────────────────────────────────────────────
kpi = run_query(queries.KPI_SUMMARY, {"start": start_date, "end": end_date})

col1, col2, col3, col4 = st.columns(4)
col1.metric("Airports Tracked", int(kpi["airports"].iloc[0]))
col2.metric("Total Flights", f"{int(kpi['total_flights'].iloc[0]):,}")
col3.metric("Days of Data", int(kpi["days_covered"].iloc[0]))
col4.metric("Avg Daily Movements", float(kpi["avg_daily_movements"].iloc[0]))

# ── Overview chart ──────────────────────────────────────────────────
st.subheader("Daily Flight Movements")

overview_df = run_query(
    queries.OVERVIEW_DAILY,
    {"start": start_date, "end": end_date},
)

if not overview_df.empty:
    fig = charts.overview_sparkline(overview_df)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No data available for the selected date range.")

# ── Pipeline info ───────────────────────────────────────────────────
with st.expander("About the Data Pipeline"):
    st.markdown("""
    **Data Flow:**
    1. **OpenSky Network API** — Flight data for 6 major airports (KJFK, EGLL, LFPG, RJTT, OMDB, YSSY)
    2. **Apache Airflow** — Orchestrates daily data extraction and loading
    3. **Snowflake** — Cloud data warehouse storing raw flight records
    4. **dbt** — Transforms data through Bronze (raw), Silver (cleaned), and Gold (analytics) layers
    5. **Streamlit** — This interactive dashboard querying the Gold layer

    **Airports:** JFK (New York), Heathrow (London), CDG (Paris),
    Haneda (Tokyo), Dubai International, Sydney International
    """)
