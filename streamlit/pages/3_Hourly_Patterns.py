"""
Hourly Flight Patterns
======================
Discover peak traffic hours and daily rhythm of flight activity.
"""

import streamlit as st

from utils.snowflake_conn import run_query, format_airport_list
from utils import queries, charts

st.header("Hourly Flight Patterns")

# ── Read shared filters ─────────────────────────────────────────────
start_date = st.session_state.get("start_date")
end_date = st.session_state.get("end_date")
selected_airports = st.session_state.get("selected_airports", [])

if not selected_airports or not start_date:
    st.warning("Please set filters on the Overview page first.")
    st.stop()

airport_clause = format_airport_list(selected_airports)

# ── Hourly bar chart ────────────────────────────────────────────────
st.subheader("Flights by Hour of Day (UTC)")

sql_hourly = queries.HOURLY_ACTIVITY.format(airports=airport_clause)
hourly_df = run_query(sql_hourly, {"start": start_date, "end": end_date})

if hourly_df.empty:
    st.info("No hourly data available for the selected filters.")
    st.stop()

fig1 = charts.hourly_bar(hourly_df)
st.plotly_chart(fig1, use_container_width=True)

# ── Peak hour insight ───────────────────────────────────────────────
peak = hourly_df.loc[hourly_df["flight_count"].idxmax()]
st.info(
    f"Peak hour: **{int(peak['departure_hour_utc']):02d}:00 UTC** "
    f"({peak['flight_direction']}s) with **{int(peak['flight_count']):,}** flights"
)

# ── Heatmap ─────────────────────────────────────────────────────────
st.subheader("Activity Heatmap (Date x Hour)")
st.caption("Darker cells = more flights. Useful for spotting daily patterns.")

sql_heatmap = queries.HOURLY_HEATMAP.format(airports=airport_clause)
heatmap_df = run_query(sql_heatmap, {"start": start_date, "end": end_date})

if not heatmap_df.empty:
    fig2 = charts.hourly_heatmap(heatmap_df)
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("Not enough data for the heatmap.")
