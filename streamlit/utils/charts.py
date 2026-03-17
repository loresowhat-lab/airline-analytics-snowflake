"""
Reusable Plotly chart builders for the Airline Analytics dashboard.

Each function takes a pandas DataFrame and returns a plotly Figure.
Pages render them with: st.plotly_chart(fig, use_container_width=True)
"""

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# ── Shared theme ────────────────────────────────────────────────────
TEMPLATE = "plotly_white"
COLOR_ARRIVALS = "#636EFA"
COLOR_DEPARTURES = "#EF553B"
COLOR_TOTAL = "#00CC96"


def daily_traffic_line(df: pd.DataFrame) -> go.Figure:
    """
    Multi-line chart of daily arrivals, departures, and total movements.

    If multiple airports, shows one line per airport for total_movements.
    """
    if df["airport_icao"].nunique() > 1:
        fig = px.line(
            df,
            x="flight_date",
            y="total_movements",
            color="airport_icao",
            markers=True,
            labels={
                "flight_date": "Date",
                "total_movements": "Total Movements",
                "airport_icao": "Airport",
            },
            template=TEMPLATE,
        )
    else:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df["flight_date"], y=df["total_arrivals"],
            name="Arrivals", line=dict(color=COLOR_ARRIVALS),
            mode="lines+markers",
        ))
        fig.add_trace(go.Scatter(
            x=df["flight_date"], y=df["total_departures"],
            name="Departures", line=dict(color=COLOR_DEPARTURES),
            mode="lines+markers",
        ))
        fig.add_trace(go.Scatter(
            x=df["flight_date"], y=df["total_movements"],
            name="Total", line=dict(color=COLOR_TOTAL, dash="dash"),
            mode="lines+markers",
        ))
        fig.update_layout(
            template=TEMPLATE,
            xaxis_title="Date",
            yaxis_title="Flight Count",
        )
    fig.update_layout(legend=dict(orientation="h", y=-0.15))
    return fig


def airport_comparison_bar(df: pd.DataFrame) -> go.Figure:
    """
    Side-by-side bar chart comparing arrivals vs departures per airport.
    """
    agg = df.groupby(["airport_icao", "airport_name"], as_index=False).agg(
        arrivals=("total_arrivals", "sum"),
        departures=("total_departures", "sum"),
    )
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=agg["airport_icao"], y=agg["arrivals"],
        name="Arrivals", marker_color=COLOR_ARRIVALS,
    ))
    fig.add_trace(go.Bar(
        x=agg["airport_icao"], y=agg["departures"],
        name="Departures", marker_color=COLOR_DEPARTURES,
    ))
    fig.update_layout(
        barmode="group",
        template=TEMPLATE,
        xaxis_title="Airport",
        yaxis_title="Total Flights",
        legend=dict(orientation="h", y=-0.15),
    )
    return fig


def top_routes_bar(df: pd.DataFrame) -> go.Figure:
    """
    Horizontal bar chart of the busiest routes.
    """
    df = df.copy()
    df["route"] = df["departure_city"] + " -> " + df["arrival_city"]
    # Reverse so the busiest is at top
    df = df.sort_values("total_flights", ascending=True).tail(20)

    fig = px.bar(
        df,
        x="total_flights",
        y="route",
        orientation="h",
        color="avg_duration_min",
        color_continuous_scale="Viridis",
        labels={
            "total_flights": "Total Flights",
            "route": "Route",
            "avg_duration_min": "Avg Duration (min)",
        },
        template=TEMPLATE,
    )
    fig.update_layout(yaxis=dict(dtick=1), height=max(400, len(df) * 28))
    return fig


def duration_scatter(df: pd.DataFrame) -> go.Figure:
    """
    Bubble chart: avg duration vs total flights, sized by unique aircraft.
    """
    df = df.copy()
    df["route"] = df["departure_city"] + " -> " + df["arrival_city"]

    fig = px.scatter(
        df,
        x="total_flights",
        y="avg_duration_min",
        size="unique_aircraft",
        hover_name="route",
        labels={
            "total_flights": "Total Flights",
            "avg_duration_min": "Avg Duration (min)",
            "unique_aircraft": "Unique Aircraft",
        },
        template=TEMPLATE,
    )
    return fig


def hourly_bar(df: pd.DataFrame) -> go.Figure:
    """
    Grouped bar chart of flight counts by hour, colored by direction.
    """
    fig = px.bar(
        df,
        x="departure_hour_utc",
        y="flight_count",
        color="flight_direction",
        barmode="group",
        color_discrete_map={
            "arrival": COLOR_ARRIVALS,
            "departure": COLOR_DEPARTURES,
        },
        labels={
            "departure_hour_utc": "Hour (UTC)",
            "flight_count": "Flight Count",
            "flight_direction": "Direction",
        },
        template=TEMPLATE,
    )
    fig.update_layout(
        xaxis=dict(dtick=1),
        legend=dict(orientation="h", y=-0.15),
    )
    return fig


def hourly_heatmap(df: pd.DataFrame) -> go.Figure:
    """
    Heatmap of flight counts: date (y-axis) x hour (x-axis).
    Reveals daily patterns and peak hours at a glance.
    """
    pivot = df.pivot_table(
        index="flight_date",
        columns="departure_hour_utc",
        values="flight_count",
        aggfunc="sum",
        fill_value=0,
    )

    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=[f"{h:02d}:00" for h in pivot.columns],
        y=[str(d) for d in pivot.index],
        colorscale="YlOrRd",
        colorbar_title="Flights",
    ))
    fig.update_layout(
        template=TEMPLATE,
        xaxis_title="Hour (UTC)",
        yaxis_title="Date",
        height=max(300, len(pivot) * 30),
    )
    return fig


def overview_sparkline(df: pd.DataFrame) -> go.Figure:
    """
    Simple area chart for the overview page showing total movements over time.
    """
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["flight_date"],
        y=df["total_movements"],
        fill="tozeroy",
        line=dict(color=COLOR_TOTAL),
        mode="lines",
    ))
    fig.update_layout(
        template=TEMPLATE,
        xaxis_title="Date",
        yaxis_title="Total Movements",
        height=300,
        margin=dict(l=40, r=20, t=20, b=40),
    )
    return fig
