"""
config.py — Central configuration loader.
Reads from .env and provides typed settings to all pipeline stages.
"""
import os
from dotenv import load_dotenv

load_dotenv()


def get_city_coords() -> dict[str, tuple[float, float]]:
    """Parse CITY_COORDS env variable into a usable dict."""
    raw = os.getenv(
        "CITY_COORDS",
        "Casablanca:33.5731:-7.5898|Paris:48.8566:2.3522"
    )
    cities = {}
    for entry in raw.split("|"):
        parts = entry.strip().split(":")
        if len(parts) == 3:
            city, lat, lon = parts
            cities[city] = (float(lat), float(lon))
    return cities


DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///data/weather_warehouse.db")
PIPELINE_INTERVAL_MINUTES: int = int(os.getenv("PIPELINE_INTERVAL_MINUTES", 60))
RAW_DATA_RETENTION_DAYS: int = int(os.getenv("RAW_DATA_RETENTION_DAYS", 7))
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
CITY_COORDS: dict = get_city_coords()

# API base URL (Open-Meteo — free, no key required)
OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"

# Variables to fetch per city
HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "precipitation",
    "weathercode",
]
