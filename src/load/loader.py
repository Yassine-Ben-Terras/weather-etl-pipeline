"""
loader.py — LOAD stage.

Persists the cleaned DataFrame to two targets:
  1. SQLite database (via SQLAlchemy) — queryable warehouse table
  2. Parquet file (via PyArrow) — columnar storage for analytics / archiving

Uses upsert logic on (city, time) to avoid duplicate rows on reruns.
"""
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from sqlalchemy import create_engine, text

from src.config import DATABASE_URL
from src.utils.logger import logger

PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

TABLE_NAME = "weather_hourly"


def load_to_sqlite(df: pd.DataFrame, db_url: str = DATABASE_URL) -> int:
    """
    Upsert DataFrame rows into the SQLite warehouse.
    Skips rows that already exist (same city + time).
    Returns count of newly inserted rows.
    """
    engine = create_engine(db_url)

    # Load existing (city, time) pairs to avoid duplicates
    with engine.connect() as conn:
        try:
            existing = pd.read_sql(
                f"SELECT city, time FROM {TABLE_NAME}", conn
            )
            existing_keys = set(
                zip(existing["city"], pd.to_datetime(existing["time"]).astype(str))
            )
        except Exception:
            existing_keys = set()

    # Filter only new rows
    df_copy = df.copy()
    df_copy["_key"] = list(zip(df_copy["city"], df_copy["time"].astype(str)))
    new_rows = df_copy[~df_copy["_key"].isin(existing_keys)].drop(columns=["_key"])

    if new_rows.empty:
        logger.info("No new rows to insert — warehouse already up to date")
        return 0

    new_rows.to_sql(
        TABLE_NAME,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )

    logger.success(f"Inserted {len(new_rows)} new rows into '{TABLE_NAME}' table")
    return len(new_rows)


def load_to_parquet(df: pd.DataFrame) -> Path:
    """
    Save the DataFrame as a timestamped Parquet file.
    Parquet is ideal for downstream analytics (Spark, DuckDB, pandas).
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    parquet_path = PROCESSED_DIR / f"weather_{timestamp}.parquet"

    df.to_parquet(parquet_path, index=False, engine="pyarrow", compression="snappy")
    size_kb = parquet_path.stat().st_size / 1024
    logger.success(f"Saved Parquet → {parquet_path} ({size_kb:.1f} KB)")
    return parquet_path


def load_all(df: pd.DataFrame) -> dict:
    """
    Run all load targets and return a summary report.
    """
    logger.info(f"Starting LOAD stage: {len(df)} rows to persist")

    if df.empty:
        logger.error("Empty DataFrame — LOAD skipped")
        return {"inserted_rows": 0, "parquet_path": None}

    inserted = load_to_sqlite(df)
    parquet_path = load_to_parquet(df)

    report = {
        "inserted_rows": inserted,
        "parquet_path": str(parquet_path),
        "total_rows_processed": len(df),
        "cities": df["city"].unique().tolist(),
    }

    logger.info(f"LOAD complete: {report}")
    return report
