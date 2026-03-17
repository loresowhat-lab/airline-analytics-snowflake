"""
SQL queries for the Streamlit dashboard.

All queries target the Gold layer tables in Snowflake.
Table names are unqualified because the connection already points
to the RAW_GOLD schema (see snowflake_conn.py).
"""

# ── Sidebar filter helpers ──────────────────────────────────────────

AVAILABLE_AIRPORTS = """
    SELECT DISTINCT airport_icao, airport_name, city, country
    FROM gld_daily_airport_traffic
    ORDER BY airport_icao
"""

DATE_RANGE = """
    SELECT MIN(flight_date) AS min_date, MAX(flight_date) AS max_date
    FROM gld_daily_airport_traffic
"""

# ── Overview page ───────────────────────────────────────────────────

KPI_SUMMARY = """
    SELECT
        COUNT(DISTINCT airport_icao)        AS airports,
        SUM(total_movements)                AS total_flights,
        COUNT(DISTINCT flight_date)         AS days_covered,
        ROUND(AVG(total_movements), 1)      AS avg_daily_movements
    FROM gld_daily_airport_traffic
    WHERE flight_date BETWEEN %(start)s AND %(end)s
"""

OVERVIEW_DAILY = """
    SELECT flight_date,
           SUM(total_arrivals)   AS total_arrivals,
           SUM(total_departures) AS total_departures,
           SUM(total_movements)  AS total_movements
    FROM gld_daily_airport_traffic
    WHERE flight_date BETWEEN %(start)s AND %(end)s
    GROUP BY flight_date
    ORDER BY flight_date
"""

# ── Daily Traffic page ──────────────────────────────────────────────

DAILY_TRAFFIC = """
    SELECT flight_date, airport_icao, airport_name,
           total_arrivals, total_departures, total_movements,
           avg_arrival_duration_min, avg_departure_duration_min
    FROM gld_daily_airport_traffic
    WHERE flight_date BETWEEN %(start)s AND %(end)s
      AND airport_icao IN ({airports})
    ORDER BY flight_date, airport_icao
"""

# ── Route Analysis page ────────────────────────────────────────────

TOP_ROUTES = """
    SELECT departure_airport_icao, arrival_airport_icao,
           departure_city, arrival_city,
           total_flights, avg_duration_min,
           unique_aircraft, days_with_service
    FROM gld_route_analysis
    ORDER BY total_flights DESC
    LIMIT %(limit)s
"""

ROUTES_FOR_AIRPORT = """
    SELECT departure_airport_icao, arrival_airport_icao,
           departure_city, arrival_city,
           total_flights, avg_duration_min,
           unique_aircraft, days_with_service
    FROM gld_route_analysis
    WHERE departure_airport_icao = %(airport)s
       OR arrival_airport_icao = %(airport)s
    ORDER BY total_flights DESC
"""

# ── Hourly Patterns page ───────────────────────────────────────────

HOURLY_ACTIVITY = """
    SELECT departure_hour_utc, flight_direction,
           SUM(flight_count)        AS flight_count,
           ROUND(AVG(avg_duration_min), 1) AS avg_duration_min
    FROM gld_hourly_flight_activity
    WHERE flight_date BETWEEN %(start)s AND %(end)s
      AND airport_icao IN ({airports})
    GROUP BY departure_hour_utc, flight_direction
    ORDER BY departure_hour_utc
"""

HOURLY_HEATMAP = """
    SELECT flight_date, departure_hour_utc,
           SUM(flight_count) AS flight_count
    FROM gld_hourly_flight_activity
    WHERE flight_date BETWEEN %(start)s AND %(end)s
      AND airport_icao IN ({airports})
    GROUP BY flight_date, departure_hour_utc
    ORDER BY flight_date, departure_hour_utc
"""
