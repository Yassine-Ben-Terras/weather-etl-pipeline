"""
extractor.py — EXTRACT stage.

Pulls hourly weather forecast data from the Open-Meteo API
for each configured city and saves raw JSON responses to disk.

Open-Meteo docs: https://open-meteo.com/en/docs
No API key required.
"""
import json
import requests
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from src.config import CITY_COORDS, HOURLY_VARIABLES, OPEN_METEO_BASE_URL
from src.utils.logger import logger

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)


def fetch_city_weather(city: str, lat: float, lon: float) -> Optional[dict]:
    """
    Call Open-Meteo API for a single city.
    Returns parsed JSON response or None on failure.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(HOURLY_VARIABLES),
        "forecast_days": 1,
        "timezone": "auto",
    }

    try:
        logger.info(f"Fetching weather for {city} (lat={lat}, lon={lon})")
        response = requests.get(OPEN_METEO_BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        data["_meta"] = {
            "city": city,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        logger.success(f"Successfully fetched {len(data.get('hourly', {}).get('time', []))} hourly records for {city}")
        return data

    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching data for {city}")
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error for {city}: {e.response.status_code} — {e}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for {city}: {e}")
    except json.JSONDecodeError:
        logger.error(f"Failed to parse JSON response for {city}")

    return None


def extract_all() -> list[dict]:
    """
    Run extraction for all cities defined in config.
    Saves each raw response as a JSON file timestamped to the minute.
    Returns list of successfully fetched payloads.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    results = []

    logger.info(f"Starting EXTRACT stage for {len(CITY_COORDS)} cities")

    for city, (lat, lon) in CITY_COORDS.items():
        payload = fetch_city_weather(city, lat, lon)
        if payload:
            # Persist raw response for auditability / reprocessing
            safe_city = city.replace(" ", "_").lower()
            raw_path = RAW_DIR / f"{safe_city}_{timestamp}.json"
            with open(raw_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            logger.debug(f"Saved raw data → {raw_path}")
            results.append(payload)

    logger.info(f"EXTRACT complete: {len(results)}/{len(CITY_COORDS)} cities fetched")
    return results
