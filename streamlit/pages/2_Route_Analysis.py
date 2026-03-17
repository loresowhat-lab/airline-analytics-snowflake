"""
Route Analysis
==============
Explore the busiest routes, flight durations, and aircraft diversity.
"""

import streamlit as st

from utils.snowflake_conn import run_query
from utils import queries, charts

st.header("Route Analysis")

# ── Controls ────────────────────────────────────────────────────────
col1, col2 = st.columns(2)
with col1:
    top_n = st.slider("Top N routes", min_value=5, max_value=50, value=15)
with col2:
    airports_df = st.session_state.get("airports_df")
    if airports_df is not None:
        drill_airport = st.selectbox(
            "Drill into airport",
            options=["All"] + airports_df["airport_icao"].tolist(),
            format_func=lambda x: (
                x if x == "All"
                else f"{x} — {airports_df[airports_df['airport_icao'] == x]['city'].iloc[0]}"
            ),
        )
    else:
        drill_airport = "All"

# ── Fetch data ──────────────────────────────────────────────────────
if drill_airport == "All":
    df = run_query(queries.TOP_ROUTES, {"limit": top_n})
else:
    df = run_query(queries.ROUTES_FOR_AIRPORT, {"airport": drill_airport})

if df.empty:
    st.info("No route data available.")
    st.stop()

# ── KPI cards ───────────────────────────────────────────────────────
k1, k2, k3 = st.columns(3)
k1.metric("Total Routes", len(df))
k2.metric("Busiest Route", f"{df.iloc[0]['departure_city']} -> {df.iloc[0]['arrival_city']}")
k3.metric(
    "Max Flights on Route",
    f"{int(df['total_flights'].max()):,}",
)

# ── Top routes bar chart ────────────────────────────────────────────
st.subheader("Busiest Routes")
fig1 = charts.top_routes_bar(df)
st.plotly_chart(fig1, use_container_width=True)

# ── Duration scatter ────────────────────────────────────────────────
st.subheader("Duration vs Volume")
st.caption("Bubble size = number of unique aircraft on the route")
fig2 = charts.duration_scatter(df)
st.plotly_chart(fig2, use_container_width=True)

# ── Data table ──────────────────────────────────────────────────────
with st.expander("View Route Data"):
    st.dataframe(
        df.rename(columns={
            "departure_airport_icao": "From (ICAO)",
            "arrival_airport_icao": "To (ICAO)",
            "departure_city": "From City",
            "arrival_city": "To City",
            "total_flights": "Flights",
            "avg_duration_min": "Avg Duration (min)",
            "unique_aircraft": "Unique Aircraft",
            "days_with_service": "Days with Service",
        }),
        use_container_width=True,
        hide_index=True,
    )
