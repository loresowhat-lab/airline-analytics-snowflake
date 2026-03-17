"""
Daily Traffic Analysis
======================
Trends in arrivals, departures, and total movements by airport and date.
"""

import streamlit as st

from utils.snowflake_conn import run_query, format_airport_list
from utils import queries, charts

st.header("Daily Airport Traffic")

# ── Read shared filters from session_state ──────────────────────────
start_date = st.session_state.get("start_date")
end_date = st.session_state.get("end_date")
selected_airports = st.session_state.get("selected_airports", [])

if not selected_airports or not start_date:
    st.warning("Please set filters on the Overview page first.")
    st.stop()

# ── Fetch data ──────────────────────────────────────────────────────
airport_clause = format_airport_list(selected_airports)
sql = queries.DAILY_TRAFFIC.format(airports=airport_clause)
df = run_query(sql, {"start": start_date, "end": end_date})

if df.empty:
    st.info("No traffic data found for the selected filters.")
    st.stop()

# ── Daily traffic line chart ────────────────────────────────────────
st.subheader("Traffic Trends")
fig = charts.daily_traffic_line(df)
st.plotly_chart(fig, use_container_width=True)

# ── Airport comparison ──────────────────────────────────────────────
st.subheader("Airport Comparison")
fig2 = charts.airport_comparison_bar(df)
st.plotly_chart(fig2, use_container_width=True)

# ── Raw data table ──────────────────────────────────────────────────
with st.expander("View Raw Data"):
    st.dataframe(
        df.rename(columns={
            "flight_date": "Date",
            "airport_icao": "Airport",
            "airport_name": "Name",
            "total_arrivals": "Arrivals",
            "total_departures": "Departures",
            "total_movements": "Total",
            "avg_arrival_duration_min": "Avg Arrival Duration (min)",
            "avg_departure_duration_min": "Avg Departure Duration (min)",
        }),
        use_container_width=True,
        hide_index=True,
    )
