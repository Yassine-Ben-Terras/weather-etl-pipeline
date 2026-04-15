"""
test_transformer.py — Unit tests for the TRANSFORM stage.
Run with: pytest tests/ -v
"""
import pandas as pd
import pytest
from datetime import datetime, timezone

# Patch config before importing transform
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.transform.transformer import (
    transform_payload,
    transform_all,
    _wind_category,
    _temp_bucket,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def make_payload(city="TestCity", n=24) -> dict:
    """Generate a valid mock API payload."""
    times = [f"2024-01-15T{h:02d}:00" for h in range(n)]
    return {
        "_meta": {
            "city": city,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        },
        "hourly": {
            "time": times,
            "temperature_2m":       [15.0 + i * 0.5 for i in range(n)],
            "relative_humidity_2m": [60 + i % 10 for i in range(n)],
            "wind_speed_10m":       [10.0 + i for i in range(n)],
            "precipitation":        [0.0 if i % 5 != 0 else 1.2 for i in range(n)],
            "weathercode":          [0] * n,
        }
    }


# ── Helper function tests ─────────────────────────────────────────────────────

class TestWindCategory:
    def test_calm(self):      assert _wind_category(0) == "Calm"
    def test_light(self):     assert _wind_category(10) == "Light breeze"
    def test_moderate(self):  assert _wind_category(30) == "Moderate breeze"
    def test_storm(self):     assert _wind_category(100) == "Storm"


class TestTempBucket:
    def test_freezing(self):     assert _temp_bucket(-5) == "Freezing"
    def test_cold(self):         assert _temp_bucket(5) == "Cold"
    def test_comfortable(self):  assert _temp_bucket(22) == "Comfortable"
    def test_hot(self):          assert _temp_bucket(40) == "Hot"


# ── transform_payload tests ───────────────────────────────────────────────────

class TestTransformPayload:

    def test_valid_payload_returns_dataframe(self):
        df = transform_payload(make_payload())
        assert df is not None
        assert isinstance(df, pd.DataFrame)

    def test_row_count_matches_input(self):
        df = transform_payload(make_payload(n=24))
        assert len(df) == 24

    def test_required_columns_present(self):
        df = transform_payload(make_payload())
        for col in ["city", "time", "temperature_c", "humidity_pct", "wind_speed_kmh"]:
            assert col in df.columns, f"Missing column: {col}"

    def test_city_name_preserved(self):
        df = transform_payload(make_payload(city="Casablanca"))
        assert (df["city"] == "Casablanca").all()

    def test_derived_columns_added(self):
        df = transform_payload(make_payload())
        assert "weather_label" in df.columns
        assert "wind_category" in df.columns
        assert "temp_bucket" in df.columns
        assert "is_rainy" in df.columns
        assert "temp_feels_like" in df.columns

    def test_is_rainy_is_binary(self):
        df = transform_payload(make_payload())
        assert set(df["is_rainy"].unique()).issubset({0, 1})

    def test_missing_hourly_returns_none(self):
        bad_payload = {"_meta": {"city": "X", "fetched_at": "2024-01-01T00:00:00Z"}}
        assert transform_payload(bad_payload) is None

    def test_no_duplicates_on_city_time(self):
        df = transform_payload(make_payload())
        assert df.duplicated(subset=["city", "time"]).sum() == 0


# ── transform_all tests ───────────────────────────────────────────────────────

class TestTransformAll:

    def test_multiple_cities_combined(self):
        payloads = [make_payload("Paris"), make_payload("London"), make_payload("Tokyo")]
        df = transform_all(payloads)
        assert df["city"].nunique() == 3

    def test_empty_input_returns_empty_df(self):
        df = transform_all([])
        assert df.empty

    def test_bad_payloads_skipped(self):
        payloads = [{"bad": "data"}, make_payload("Casablanca")]
        df = transform_all(payloads)
        assert not df.empty
        assert "Casablanca" in df["city"].values
