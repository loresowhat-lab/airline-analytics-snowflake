"""
Snowflake connection helper for Streamlit.

Uses @st.cache_resource to keep a single connection alive across reruns,
and @st.cache_data to cache query results for 5 minutes.
"""

import os

import pandas as pd
import snowflake.connector
import streamlit as st


# ── Schema mapping ──────────────────────────────────────────────────
# dbt creates schemas as {target_schema}_{custom_schema}.
# For example, with SNOWFLAKE_SCHEMA=RAW and +schema: gold in dbt_project.yml,
# the gold tables end up in RAW_GOLD.  We build these at module level.
_BASE_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "RAW")
SCHEMA_GOLD = f"{_BASE_SCHEMA}_GOLD"
SCHEMA_SILVER = f"{_BASE_SCHEMA}_SILVER"
SCHEMA_BRONZE = f"{_BASE_SCHEMA}_BRONZE"


@st.cache_resource
def get_connection():
    """
    Create and cache a Snowflake connection.

    The connection points to the GOLD schema by default because
    all dashboard queries read from the Gold layer.
    """
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=SCHEMA_GOLD,
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


@st.cache_data(ttl=300)
def run_query(sql: str, params: dict = None) -> pd.DataFrame:
    """
    Execute a SQL query and return results as a DataFrame.

    Results are cached for 5 minutes (ttl=300) so repeated page
    interactions don't re-query Snowflake unnecessarily.
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        columns = [desc[0].lower() for desc in cur.description]
        data = cur.fetchall()
        return pd.DataFrame(data, columns=columns)
    finally:
        cur.close()


def format_airport_list(airports: list[str]) -> str:
    """
    Format a list of ICAO codes for use in a SQL IN clause.

    Example: ['KJFK', 'EGLL'] → "'KJFK','EGLL'"
    """
    return ",".join(f"'{a}'" for a in airports)
