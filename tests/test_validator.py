"""
test_validator.py — Unit tests for data quality validation.
"""
import pandas as pd
import pytest
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.utils.validator import validate


def make_valid_df(n=5) -> pd.DataFrame:
    return pd.DataFrame({
        "city":             ["Paris"] * n,
        "time":             pd.date_range("2024-01-15", periods=n, freq="h"),
        "temperature_c":    [20.0] * n,
        "humidity_pct":     [55.0] * n,
        "wind_speed_kmh":   [15.0] * n,
        "precipitation_mm": [0.0] * n,
        "weather_code":     [0] * n,
    })


class TestValidator:

    def test_valid_df_passes(self):
        report = validate(make_valid_df())
        assert report.passed

    def test_empty_df_fails(self):
        report = validate(pd.DataFrame())
        assert not report.passed

    def test_missing_column_fails(self):
        df = make_valid_df().drop(columns=["temperature_c"])
        report = validate(df)
        assert not report.passed
        assert any("temperature_c" in e for e in report.errors)

    def test_null_temperature_fails(self):
        df = make_valid_df()
        df.loc[0, "temperature_c"] = None
        report = validate(df)
        assert not report.passed

    def test_out_of_range_humidity_warns(self):
        df = make_valid_df()
        df.loc[0, "humidity_pct"] = 150  # impossible value
        report = validate(df)
        assert any("humidity_pct" in w for w in report.warnings)
