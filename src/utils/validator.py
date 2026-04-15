"""
validator.py — Data quality gate between TRANSFORM and LOAD.

Runs lightweight schema and range checks on the cleaned DataFrame.
If critical checks fail, the pipeline aborts before hitting the database.

Checks performed:
  - Required columns present
  - No nulls in critical fields
  - Temperature within realistic range (-90°C to 60°C)
  - Humidity between 0–100%
  - Wind speed non-negative
  - At least one row per city
"""
import pandas as pd
from dataclasses import dataclass, field
from src.utils.logger import logger


@dataclass
class ValidationReport:
    passed: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def fail(self, msg: str):
        self.passed = False
        self.errors.append(msg)

    def warn(self, msg: str):
        self.warnings.append(msg)

    def summary(self) -> str:
        status = "✅ PASSED" if self.passed else "❌ FAILED"
        lines = [f"Validation {status}"]
        if self.errors:
            lines += [f"  ERROR: {e}" for e in self.errors]
        if self.warnings:
            lines += [f"  WARN:  {w}" for w in self.warnings]
        return "\n".join(lines)


REQUIRED_COLUMNS = [
    "city", "time", "temperature_c", "humidity_pct",
    "wind_speed_kmh", "precipitation_mm", "weather_code",
]

NUMERIC_RANGES = {
    "temperature_c":    (-90, 60),
    "humidity_pct":     (0, 100),
    "wind_speed_kmh":   (0, 400),
    "precipitation_mm": (0, 500),
}


def validate(df: pd.DataFrame) -> ValidationReport:
    report = ValidationReport()

    # ── 1. Schema check ───────────────────────────────────────────────────────
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        report.fail(f"Missing required columns: {missing_cols}")
        return report  # Can't continue without schema

    # ── 2. Empty DataFrame ────────────────────────────────────────────────────
    if df.empty:
        report.fail("DataFrame is empty — nothing to load")
        return report

    # ── 3. Nulls in critical columns ─────────────────────────────────────────
    for col in ["city", "time", "temperature_c"]:
        null_count = df[col].isna().sum()
        if null_count > 0:
            report.fail(f"Column '{col}' has {null_count} null values")

    # ── 4. Numeric range checks ───────────────────────────────────────────────
    for col, (lo, hi) in NUMERIC_RANGES.items():
        if col not in df.columns:
            continue
        out_of_range = df[(df[col] < lo) | (df[col] > hi)].shape[0]
        if out_of_range > 0:
            report.warn(f"Column '{col}': {out_of_range} rows outside expected range [{lo}, {hi}]")

    # ── 5. City coverage ──────────────────────────────────────────────────────
    row_counts = df.groupby("city").size()
    empty_cities = row_counts[row_counts == 0].index.tolist()
    if empty_cities:
        report.warn(f"Cities with 0 rows after transform: {empty_cities}")

    # ── 6. Duplicate check ────────────────────────────────────────────────────
    dupes = df.duplicated(subset=["city", "time"]).sum()
    if dupes > 0:
        report.warn(f"{dupes} duplicate (city, time) combinations found")

    logger.info(report.summary())
    return report
