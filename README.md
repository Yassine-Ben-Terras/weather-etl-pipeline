# Weather Data Engineering Pipeline

A production-grade **ETL pipeline** that ingests real-time weather data from the [Open-Meteo API](https://open-meteo.com/) for multiple cities, transforms and enriches it, validates data quality, then persists it to both a SQLite warehouse and Parquet files.

> **Tech stack:** Python · Pandas · SQLAlchemy · PyArrow · Loguru · Pytest · Open-Meteo API  
> **Level:** Intermediate  
> **Pattern:** Extract → Validate → Transform → Validate → Load

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                       pipeline.py (Orchestrator)                │
└──────────────────┬──────────────────────────────────────────────┘
                   │
        ┌──────────▼──────────┐
        │   EXTRACT           │  src/extract/extractor.py
        │   Open-Meteo API    │  → Fetches hourly forecast for 5 cities
        │   → Raw JSON files  │  → Saves raw JSON to data/raw/
        └──────────┬──────────┘
                   │
        ┌──────────▼──────────┐
        │   TRANSFORM         │  src/transform/transformer.py
        │   Flatten + Clean   │  → Flatten nested hourly arrays
        │   Enrich + Derive   │  → Cast types, drop nulls, deduplicate
        └──────────┬──────────┘  → Add: feels_like, wind_category,
                   │               temp_bucket, is_rainy, weather_label
        ┌──────────▼──────────┐
        │   VALIDATE          │  src/utils/validator.py
        │   Quality Gate      │  → Schema checks, range checks
        │                     │  → Fails pipeline if critical errors
        └──────────┬──────────┘
                   │
        ┌──────────▼──────────┐
        │   LOAD              │  src/load/loader.py
        │   SQLite (upsert)   │  → Upsert into weather_hourly table
        │   Parquet archive   │  → Snappy-compressed Parquet file
        └─────────────────────┘
```

---

## 🗂️ Project Structure

```
weather_pipeline/
│
├── pipeline.py                   # ← Main entry point
├── requirements.txt
├── .env                          # Configuration (cities, DB URL, schedule)
│
├── src/
│   ├── config.py                 # Centralized settings loader
│   ├── extract/
│   │   └── extractor.py          # EXTRACT: API calls + raw file saving
│   ├── transform/
│   │   └── transformer.py        # TRANSFORM: cleaning + enrichment
│   ├── load/
│   │   └── loader.py             # LOAD: SQLite upsert + Parquet write
│   └── utils/
│       ├── logger.py             # Structured logging (console + file)
│       └── validator.py          # Data quality validation
│
├── data/
│   ├── raw/                      # Raw JSON responses (per-city, timestamped)
│   └── processed/                # Parquet files (timestamped)
│
├── logs/                         # Rotating daily log files
└── tests/
    ├── test_transformer.py       # 19 unit tests for transform logic
    └── test_validator.py         # 5 unit tests for data quality checks
```

---

## Setup

### 1. Clone and create virtual environment

```bash
git clone https://github.com/Yassine-Ben-Terras/weather-etl-pipeline.git
cd weather_pipeline
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure `.env`

Edit `.env` to customize:

```env
# Cities to track (must match CITY_COORDS entries)
CITIES=Casablanca,Paris,London,New York,Tokyo

# Coordinate pairs used by the extractor
CITY_COORDS=Casablanca:33.5731:-7.5898|Paris:48.8566:2.3522|...

# SQLite database location
DATABASE_URL=sqlite:///data/weather_warehouse.db

# How often to run in scheduled mode (minutes)
PIPELINE_INTERVAL_MINUTES=60

# Logging verbosity
LOG_LEVEL=INFO
```

> No API key required — Open-Meteo is free and open.

---

## Running the Pipeline

### Single run

```bash
python pipeline.py
```

### Scheduled (runs every N minutes, defined in `.env`)

```bash
python pipeline.py --schedule
```

### Expected output

```
2024-01-15 10:00:00 | INFO     | pipeline — Pipeline run started — ID: 20240115_100000
2024-01-15 10:00:00 | INFO     | extractor — Starting EXTRACT stage for 5 cities
2024-01-15 10:00:01 | SUCCESS  | extractor — Successfully fetched 24 hourly records for Casablanca
...
2024-01-15 10:00:03 | INFO     | transformer — TRANSFORM complete: 120 total rows across 5 cities
2024-01-15 10:00:03 | INFO     | validator — Validation ✅ PASSED
2024-01-15 10:00:03 | SUCCESS  | loader — Inserted 120 new rows into 'weather_hourly' table
2024-01-15 10:00:03 | SUCCESS  | loader — Saved Parquet → data/processed/weather_20240115_100003.parquet (14.2 KB)
2024-01-15 10:00:03 | INFO     | pipeline — Pipeline run COMPLETE in 2.14s
```

---

## Running Tests

```bash
pytest tests/ -v
```

```
24 passed in 1.77s
```

Tests cover: wind categorization, temperature bucketing, DataFrame schema validation, null handling, deduplication, multi-city aggregation, and data quality gate logic.

---

## Querying the Warehouse

After at least one pipeline run, query the SQLite database directly:

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("sqlite:///data/weather_warehouse.db")

# Latest temperatures per city
df = pd.read_sql("""
    SELECT city, time, temperature_c, temp_bucket, weather_label
    FROM weather_hourly
    ORDER BY time DESC
    LIMIT 50
""", engine)

print(df)
```

Or use the Parquet files with DuckDB for analytics:

```python
import duckdb

duckdb.query("""
    SELECT city,
           AVG(temperature_c)    AS avg_temp,
           MAX(wind_speed_kmh)   AS max_wind,
           SUM(precipitation_mm) AS total_rain
    FROM 'data/processed/*.parquet'
    GROUP BY city
    ORDER BY avg_temp DESC
""").df()
```

---

## Data Dictionary

| Column | Type | Description |
|---|---|---|
| `city` | str | City name |
| `time` | datetime | Forecast hour (local timezone) |
| `temperature_c` | float | Air temperature at 2m (°C) |
| `temp_feels_like` | float | Apparent temperature (°C) |
| `temp_bucket` | str | Freezing / Cold / Cool / Comfortable / Warm / Hot |
| `humidity_pct` | float | Relative humidity (%) |
| `wind_speed_kmh` | float | Wind speed at 10m (km/h) |
| `wind_category` | str | Calm / Light breeze / … / Storm |
| `precipitation_mm` | float | Hourly precipitation (mm) |
| `is_rainy` | int | 1 if precipitation > 0.1mm, else 0 |
| `weather_code` | int | WMO weather interpretation code |
| `weather_label` | str | Human-readable weather description |
| `fetched_at` | datetime | UTC timestamp of API call |
| `pipeline_run_ts` | datetime | UTC timestamp of this pipeline run |

---

## Extending the Pipeline

| What to do | Where to look |
|---|---|
| Add a new city | Update `CITY_COORDS` in `.env` |
| Add new API variables | `HOURLY_VARIABLES` in `src/config.py`, then `transformer.py` |
| Switch to PostgreSQL | Change `DATABASE_URL` in `.env` — SQLAlchemy handles the rest |
| Add email alerts on failure | Wrap `run_once()` in `pipeline.py` with a try/except + SMTP call |
| Add more quality checks | Extend `validate()` in `src/utils/validator.py` |
| Export to CSV | Add `df.to_csv(...)` call in `src/load/loader.py` |

---

## 📋 Key Concepts Demonstrated

- **ETL pattern** — Clear separation of Extract, Transform, Load stages
- **Data quality gates** — Pipeline aborts before loading if validation fails
- **Idempotency** — Upsert logic prevents duplicate rows on reruns
- **Observability** — Structured logging to console + rotating log files
- **Raw data preservation** — Original API responses saved as JSON for reprocessing
- **Columnar storage** — Parquet output for efficient downstream analytics
- **Unit testing** — 24 tests covering business logic without needing network calls

---

## 📦 Dependencies

| Package | Purpose |
|---|---|
| `requests` | HTTP client for API calls |
| `pandas` | DataFrame operations (transform, dedup, type casting) |
| `sqlalchemy` | Database ORM (SQLite, easily swappable to Postgres) |
| `pyarrow` | Parquet read/write |
| `loguru` | Structured, colored logging with file rotation |
| `python-dotenv` | `.env` configuration loading |
| `schedule` | In-process job scheduling |
| `pytest` | Unit testing framework |

---

## 📄 License

MIT — free to use and modify for learning or production.
