"""
transformer.py — TRANSFORM stage.

Takes raw API payloads, flattens nested JSON into a tidy DataFrame,
applies data quality checks, and adds derived/enriched columns.

Transformations applied:
  1. Flatten nested hourly arrays → one row per hour per city
  2. Parse timestamps to proper datetime
  3. Cast numeric columns to correct dtypes
  4. Add derived columns: feels_like bucket, wind category, is_rainy flag
  5. Drop rows with null critical fields (temperature, time)
  6. Deduplicate on (city, time)
"""
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import Optional

from src.utils.logger import logger

# WMO weather interpretation codes → human-readable label
WMO_CODE_MAP = {
    0: "Clear sky",
    1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Foggy", 48: "Depositing rime fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
    80: "Slight showers", 81: "Moderate showers", 82: "Violent showers",
    95: "Thunderstorm", 96: "Thunderstorm + hail", 99: "Thunderstorm + heavy hail",
}


def _wind_category(speed_kmh: float) -> str:
    """Beaufort-inspired wind label from speed in km/h."""
    if speed_kmh < 1:    return "Calm"
    if speed_kmh < 20:   return "Light breeze"
    if speed_kmh < 40:   return "Moderate breeze"
    if speed_kmh < 60:   return "Fresh breeze"
    if speed_kmh < 80:   return "Strong breeze"
    return "Storm"


def _temp_bucket(temp_c: float) -> str:
    """Classify temperature into human-readable thermal comfort band."""
    if temp_c < 0:    return "Freezing"
    if temp_c < 10:   return "Cold"
    if temp_c < 18:   return "Cool"
    if temp_c < 25:   return "Comfortable"
    if temp_c < 35:   return "Warm"
    return "Hot"


def transform_payload(payload: dict) -> Optional[pd.DataFrame]:
    """
    Transform a single city's raw API response into a clean DataFrame.
    Returns None if the payload is malformed.
    """
    city = payload.get("_meta", {}).get("city", "Unknown")
    fetched_at = payload.get("_meta", {}).get("fetched_at")
    hourly = payload.get("hourly", {})

    if not hourly or "time" not in hourly:
        logger.warning(f"[{city}] Missing hourly data — skipping")
        return None

    try:
        df = pd.DataFrame({
            "time":              hourly.get("time", []),
            "temperature_c":     hourly.get("temperature_2m", []),
            "humidity_pct":      hourly.get("relative_humidity_2m", []),
            "wind_speed_kmh":    hourly.get("wind_speed_10m", []),
            "precipitation_mm":  hourly.get("precipitation", []),
            "weather_code":      hourly.get("weathercode", []),
        })

        # ── Type casting ──────────────────────────────────────────────────────
        df["time"] = pd.to_datetime(df["time"])
        for col in ["temperature_c", "humidity_pct", "wind_speed_kmh", "precipitation_mm"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["weather_code"] = df["weather_code"].fillna(-1).astype(int)

        # ── Data quality: drop rows missing critical fields ───────────────────
        before = len(df)
        df.dropna(subset=["time", "temperature_c"], inplace=True)
        dropped = before - len(df)
        if dropped:
            logger.warning(f"[{city}] Dropped {dropped} rows with null critical fields")

        # ── Metadata columns ─────────────────────────────────────────────────
        df["city"] = city
        df["fetched_at"] = pd.to_datetime(fetched_at, utc=True)
        df["pipeline_run_ts"] = datetime.now(timezone.utc)

        # ── Derived / enriched columns ────────────────────────────────────────
        df["weather_label"]   = df["weather_code"].map(WMO_CODE_MAP).fillna("Unknown")
        df["wind_category"]   = df["wind_speed_kmh"].apply(_wind_category)
        df["temp_bucket"]     = df["temperature_c"].apply(_temp_bucket)
        df["is_rainy"]        = (df["precipitation_mm"] > 0.1).astype(int)
        df["temp_feels_like"] = (
            df["temperature_c"]
            - 0.4 * (df["temperature_c"] - 10) * (1 - df["humidity_pct"] / 100)
        ).round(2)

        # ── Deduplication ─────────────────────────────────────────────────────
        before = len(df)
        df.drop_duplicates(subset=["city", "time"], keep="last", inplace=True)
        if (before - len(df)) > 0:
            logger.debug(f"[{city}] Removed {before - len(df)} duplicate rows")

        # ── Column ordering ───────────────────────────────────────────────────
        df = df[[
            "city", "time", "temperature_c", "temp_feels_like", "temp_bucket",
            "humidity_pct", "wind_speed_kmh", "wind_category",
            "precipitation_mm", "is_rainy",
            "weather_code", "weather_label",
            "fetched_at", "pipeline_run_ts",
        ]]

        logger.success(f"[{city}] Transformed → {len(df)} clean rows")
        return df

    except Exception as e:
        logger.exception(f"[{city}] Transformation failed: {e}")
        return None


def transform_all(payloads: list[dict]) -> pd.DataFrame:
    """
    Run transform on all extracted payloads.
    Returns a single combined DataFrame ready for loading.
    """
    logger.info(f"Starting TRANSFORM stage for {len(payloads)} payloads")
    frames = []

    for payload in payloads:
        df = transform_payload(payload)
        if df is not None and not df.empty:
            frames.append(df)

    if not frames:
        logger.error("TRANSFORM produced no data — aborting")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    logger.info(f"TRANSFORM complete: {len(combined)} total rows across {combined['city'].nunique()} cities")
    return combined
