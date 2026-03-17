# Airline Analytics with Snowflake

End-to-end analytics engineering project: **OpenSky API → Airflow → Snowflake → dbt (medallion architecture)**.

## Architecture

```
OpenSky Network API
        │
        ▼
  ┌───────────┐       ┌────────────────────────────────────┐
  │  Airflow   │──────▶│           Snowflake                │
  │  (Docker)  │       │                                    │
  └───────────┘       │  RAW_FLIGHTS (ingestion)           │
        │              │       │                            │
        ▼              │  ┌────▼─────┐                      │
   dbt Transform       │  │  Bronze  │  brz_raw_flights     │
                       │  └────┬─────┘                      │
                       │  ┌────▼─────┐                      │
                       │  │  Silver  │  slv_flights          │
                       │  │          │  slv_airports          │
                       │  └────┬─────┘                      │
                       │  ┌────▼─────┐                      │
                       │  │   Gold   │  gld_daily_airport_   │
                       │  │          │    traffic             │
                       │  │          │  gld_route_analysis    │
                       │  │          │  gld_hourly_flight_    │
                       │  │          │    activity             │
                       │  └──────────┘                      │
                       └────────────────────────────────────┘
```

## Project Structure

```
├── docker-compose.yaml          # Airflow services + Postgres
├── Dockerfile                   # Custom Airflow image with dbt + Snowflake
├── requirements.txt             # Python dependencies
├── .env.example                 # Credential template
├── dags/
│   ├── opensky_flights_dag.py   # DAG 1: Extract flights → Load to Snowflake
│   └── dbt_transform_dag.py     # DAG 2: dbt seed → run → test
├── include/
│   ├── opensky_client.py        # OpenSky API client (OAuth2)
│   └── snowflake_loader.py      # Snowflake RAW_FLIGHTS loader
└── dbt/
    ├── dbt_project.yml
    ├── profiles.yml
    ├── packages.yml
    ├── seeds/
    │   └── seed_airports.csv    # Airport reference data
    └── models/
        ├── bronze/              # Raw staging views
        ├── silver/              # Cleaned + typed tables
        └── gold/                # Business-ready aggregations
```

## Quick Start

### 1. Prerequisites

- Docker & Docker Compose
- An OpenSky Network account with an API client ([create one here](https://opensky-network.org))
- A Snowflake account

### 2. Configure Credentials

```bash
cp .env.example .env
# Edit .env with your OpenSky and Snowflake credentials
```

### 3. Start Services

```bash
docker compose up -d
```

### 4. Access Airflow

Open [http://localhost:8080](http://localhost:8080) — login with `admin` / `admin`.

### 5. Run the Pipeline

1. **Trigger `opensky_flights_to_snowflake`** — extracts previous day's arrivals & departures for 6 global airports and loads into `RAW_FLIGHTS`
2. **Trigger `dbt_transform`** — seeds airport reference data, then builds bronze → silver → gold models

### 6. Verify in Snowflake

```sql
-- Check raw data landed
SELECT COUNT(*) FROM RAW.RAW_FLIGHTS;

-- Check gold layer
SELECT * FROM GOLD.GLD_DAILY_AIRPORT_TRAFFIC ORDER BY flight_date DESC LIMIT 10;
SELECT * FROM GOLD.GLD_ROUTE_ANALYSIS ORDER BY total_flights DESC LIMIT 10;
```

## Data Source

**OpenSky Network API** — free, open-source flight tracking data.

- Endpoints: `/flights/arrival` and `/flights/departure`
- Airports monitored: KJFK, EGLL, LFPG, RJTT, OMDB, YSSY
- Data is batch-updated nightly (previous day's flights)

## dbt Models

| Layer  | Model                        | Description                                              |
|--------|------------------------------|----------------------------------------------------------|
| Bronze | `brz_raw_flights`            | Direct view over raw ingested data                       |
| Silver | `slv_flights`                | Cleaned records with surrogate key, computed duration    |
| Silver | `slv_airports`               | Airport dimension from seed CSV                          |
| Gold   | `gld_daily_airport_traffic`  | Daily arrivals, departures, and duration stats per airport|
| Gold   | `gld_route_analysis`         | Aggregated metrics per route pair                        |
| Gold   | `gld_hourly_flight_activity` | Hourly flight counts by airport and direction            |
