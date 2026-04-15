"""
pipeline.py — Pipeline orchestrator.

Wires together Extract → Validate → Transform → Validate → Load.
Can be run once (run_once) or on a schedule (run_scheduled).

Usage:
  python pipeline.py              # single run
  python pipeline.py --schedule   # recurring run every N minutes (from .env)
"""
import sys
import time
import argparse
from datetime import datetime, timezone

import schedule

from src.extract.extractor import extract_all
from src.transform.transformer import transform_all
from src.load.loader import load_all
from src.utils.validator import validate
from src.utils.logger import logger
from src.config import PIPELINE_INTERVAL_MINUTES


def run_once() -> dict:
    """Execute a single full ETL run. Returns a run summary dict."""
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info(f"{'='*60}")
    logger.info(f"Pipeline run started — ID: {run_id}")
    logger.info(f"{'='*60}")
    start = time.perf_counter()

    # ── EXTRACT ───────────────────────────────────────────────────────────────
    raw_payloads = extract_all()
    if not raw_payloads:
        logger.error("Extract returned no data — aborting pipeline run")
        return {"run_id": run_id, "status": "FAILED", "reason": "empty_extract"}

    # ── TRANSFORM ─────────────────────────────────────────────────────────────
    clean_df = transform_all(raw_payloads)
    if clean_df.empty:
        logger.error("Transform returned empty DataFrame — aborting")
        return {"run_id": run_id, "status": "FAILED", "reason": "empty_transform"}

    # ── VALIDATE ──────────────────────────────────────────────────────────────
    report = validate(clean_df)
    if not report.passed:
        logger.error(f"Data quality gate FAILED — pipeline aborted:\n{report.summary()}")
        return {"run_id": run_id, "status": "FAILED", "reason": "validation_failed", "errors": report.errors}

    # ── LOAD ──────────────────────────────────────────────────────────────────
    load_report = load_all(clean_df)

    elapsed = time.perf_counter() - start
    summary = {
        "run_id": run_id,
        "status": "SUCCESS",
        "duration_seconds": round(elapsed, 2),
        **load_report,
    }

    logger.info(f"{'='*60}")
    logger.info(f"Pipeline run COMPLETE in {elapsed:.2f}s — {summary}")
    logger.info(f"{'='*60}")
    return summary


def run_scheduled():
    """Run the pipeline on a recurring schedule defined in .env."""
    interval = PIPELINE_INTERVAL_MINUTES
    logger.info(f"Scheduler started — pipeline will run every {interval} minutes")

    schedule.every(interval).minutes.do(run_once)
    run_once()  # Run immediately on start

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weather Data Engineering Pipeline")
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Run on a recurring schedule (interval from .env)"
    )
    args = parser.parse_args()

    if args.schedule:
        run_scheduled()
    else:
        result = run_once()
        sys.exit(0 if result["status"] == "SUCCESS" else 1)
