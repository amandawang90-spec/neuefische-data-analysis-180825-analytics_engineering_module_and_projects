# ✈️ Flight & Weather Analytics Pipeline

A full ELT pipeline that ingests weather data from the Meteostat API and US flight data into PostgreSQL, then transforms and models it using dbt for analytical reporting.

---

## 📋 Project Overview

This project combines **US domestic flight data** with **weather observations** at major airports to enable analysis of how weather conditions correlate with flight delays, cancellations, and diversions. The pipeline covers January–March 2024 for three major US airports: **JFK**, **MIA**, and **LAX**.

The architecture follows the **ELT pattern**:
1. **Extract & Load** — Jupyter notebooks call the Meteostat API and load raw JSON into PostgreSQL
2. **Transform** — dbt models clean, flatten, and enrich the data through layered staging, prep, and mart models

---

## 🗂️ Project Structure

```
├── notebooks/                          # Data ingestion (Extract & Load)
│   ├── meteostat_daily_fromAPI_to_DB.ipynb
│   ├── meteostat_hourly_fromAPI_to_DB.ipynb
│   └── meteostat_monthly_fromAPI_to_DB.ipynb
│
├── models/
│   ├── staging/                        # Raw source layer
│   │   ├── staging_sources.yml         # Source definitions
│   │   ├── stg_airports.sql
│   │   ├── stg_flights_one_month.sql
│   │   ├── stg_weather_daily.sql
│   │   └── stg_weather_hourly.sql
│   │
│   ├── prep/                           # Cleaned & enriched layer
│   │   ├── schema.yml
│   │   ├── prep_airports.sql
│   │   ├── prep_flights.sql
│   │   ├── prep_weather_daily.sql
│   │   ├── prep_weather_hourly.sql
│   │   └── prep_weather_summary.sql
│   │
│   └── marts/                          # Analytical output layer
│       ├── schema.yml
│       ├── mart_faa_stats.sql
│       ├── mart_route_stats.sql
│       ├── mart_selected_faa_stats_weather.sql
│       └── mart_weather_weekly.sql
```

---

## 🔧 Prerequisites

- Python 3.8+
- PostgreSQL database
- A [RapidAPI](https://rapidapi.com/) account with access to the [Meteostat API](https://rapidapi.com/meteostat/api/meteostat)
- dbt (with the `dbt-postgres` adapter)

### Python Dependencies

```bash
pip install pandas sqlalchemy psycopg2-binary requests python-dotenv
```

---

## ⚙️ Configuration

Create a `.env` file in the project root with the following variables:

```env
X-RapidAPI-Key=your_rapidapi_key_here
POSTGRES_USER=your_db_user
POSTGRES_PASS=your_db_password
POSTGRES_HOST=your_db_host
POSTGRES_PORT=5432
POSTGRES_DB=your_database_name
POSTGRES_SCHEMA=your_schema_name
```

---

## 🚀 Running the Pipeline

### Step 1 — Extract & Load (Jupyter Notebooks)

Run the notebooks in order to populate the raw tables in your PostgreSQL schema:

| Notebook | Output Table | Description |
|---|---|---|
| `meteostat_daily_fromAPI_to_DB.ipynb` | `weather_daily_raw` | One JSON row per airport with daily aggregates |
| `meteostat_hourly_fromAPI_to_DB.ipynb` | `weather_hourly_raw` | One JSON row per airport per month with hourly observations |
| `meteostat_monthly_fromAPI_to_DB.ipynb` | `weather_monthly_raw` | One JSON row per airport with monthly summaries |

Each notebook:
- Reads credentials from `.env`
- Calls the Meteostat RapidAPI endpoint for JFK, MIA, and LAX
- Stores the raw JSON response as-is in PostgreSQL using `pandas + SQLAlchemy`

> ⚠️ The hourly notebook makes **9 API calls** (3 airports × 3 months) due to a 30-day limit on the hourly endpoint. A `time.sleep(0.34)` delay is included between calls to respect rate limits.

### Step 2 — Transform (dbt)

Run dbt to build all models in sequence:

```bash
dbt run
dbt test
```

---

## 🧱 dbt Model Layers

### Staging (`stg_`)

Reads directly from source tables. No business logic — just basic selection and JSON flattening.

| Model | Source | Description |
|---|---|---|
| `stg_airports` | `airports`, `regions_2` | Joins airports with region metadata |
| `stg_flights_one_month` | `flights` | Filters flights to Jan 2024 (view) |
| `stg_weather_daily` | `weather_daily_raw` | Flattens daily JSON into typed columns |
| `stg_weather_hourly` | `weather_hourly_raw` | Flattens hourly JSON into typed columns |

### Prep (`prep_`)

Materialized as **tables**. Adds derived features, calendar breakdowns, and business-meaningful transformations.

| Model | Description |
|---|---|
| `prep_airports` | Reordered columns, ready for reporting joins |
| `prep_flights` | Adds `time`, `dep_delay_interval`, and `distance_km` (miles → km) |
| `prep_weather_daily` | Adds `day`, `week`, `month`, `year`, `day_name`, `month_name`, and `season` |
| `prep_weather_hourly` | Adds calendar parts plus `day_part` (night / day / evening) |
| `prep_weather_summary` | Monthly aggregates of avg temperature and total precipitation per airport |

### Marts (`mart_`)

Analytical output models combining multiple prep tables for reporting and visualization.

| Model | Description |
|---|---|
| `mart_faa_stats` | Aggregated departure and arrival stats per airport (connections, cancellations, diversions, unique airlines/planes) |
| `mart_route_stats` | Stats per origin–destination pair including avg/min/max arrival delay and elapsed time |
| `mart_selected_faa_stats_weather` | Daily airport stats joined with weather conditions — for delay vs. weather correlation analysis |
| `mart_weather_weekly` | Weekly aggregations of all weather metrics per airport and station |

---

## ✅ dbt Tests

Tests are defined in `schema.yml` files across layers:

| Model | Tested Columns | Tests |
|---|---|---|
| `prep_airports` | `faa`, `country` | `not_null`, `unique` |
| `prep_weather_daily` | `date`, `season` | `not_null`, `accepted_values` |
| `prep_weather_hourly` | `timestamp`, `day_part` | `not_null`, `accepted_values` |
| `prep_flights` | `flight_date`, `distance_km` | `not_null` |

---

## 🛫 Data Sources

| Source | Schema | Tables |
|---|---|---|
| Weather | `s_jingwang` | `weather_daily_raw`, `weather_hourly_raw` |
| Flights | `s_jingwang` | `flights`, `airports`, `regions_2` |

### Airports Covered

| Code | Airport | Meteostat Station ID |
|---|---|---|
| JFK | John F. Kennedy International | 74486 |
| MIA | Miami International | 72202 |
| LAX | Los Angeles International | 72295 |

### Time Period

**2024-01-01 to 2024-03-31** (Q1 2024)

---

## 📐 Key Design Decisions

- **ELT over ETL** — raw JSON is stored first, then transformed in dbt, keeping the raw data always available for reprocessing.
- **Schema parameter in `.to_sql()`** — uses the explicit `schema=` parameter rather than `SET search_path`, which is more portable and avoids session-state dependencies.
- **Layered dbt architecture** — staging → prep → mart gives clear separation between raw cleaning, enrichment, and analytics-facing models.
- **Materialization strategy** — staging models are views (lightweight); prep and mart models are tables (performance-optimized for reporting).
