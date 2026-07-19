"""
Form 4 (SEC insider-trading) pipeline entry point.

Downloads SEC EDGAR's quarterly Form 3/4/5 insider-transaction bulk files,
filters them to a configured universe of issuer CIKs, cleans/enriches them,
and appends the result to a single output CSV one quarter at a time. A
checkpoint file records which quarters have already been fully processed, so
an interrupted run resumes at the next unprocessed quarter instead of
restarting from scratch.

Usage:
    python main.py --config config.yaml
    python main.py --config config.yaml --start-quarter 2024Q1 --end-quarter 2024Q4
"""
import argparse
import json
import logging
import os
import time
from pathlib import Path

import yaml

from collect import QuarterNotAvailable, collect_quarter, fetch_quarter_zip, load_universe
from postprocess import process_quarter

logger = logging.getLogger(__name__)


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def setup_logging(log_file: str) -> None:
    log_dir = os.path.dirname(log_file)
    if log_dir:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )


def load_checkpoint(checkpoint_file: str) -> dict:
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            state = json.load(f)
        logger.info("Resuming from checkpoint: %d quarter(s) already done", len(state.get("completed_quarters", [])))
        return state
    return {"completed_quarters": []}


def save_checkpoint(checkpoint_file: str, state: dict) -> None:
    checkpoint_dir = os.path.dirname(checkpoint_file)
    if checkpoint_dir:
        Path(checkpoint_dir).mkdir(parents=True, exist_ok=True)
    with open(checkpoint_file, "w") as f:
        json.dump(state, f)


def iter_quarters(start_year: int, start_qtr: int, end_year: int, end_qtr: int):
    year, qtr = start_year, start_qtr
    while (year, qtr) <= (end_year, end_qtr):
        yield year, qtr
        qtr += 1
        if qtr > 4:
            qtr, year = 1, year + 1


def _parse_quarter_label(label: str):
    # e.g. "2024Q1" -> (2024, 1)
    year_str, qtr_str = label.split("Q")
    return int(year_str), int(qtr_str)


def main():
    parser = argparse.ArgumentParser(description="Collect SEC Form 4 insider-transaction data")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML (see config.example.yaml)")
    parser.add_argument("--start-quarter", help="Override run.start_year/start_quarter, e.g. 2024Q1")
    parser.add_argument("--end-quarter", help="Override run.end_year/end_quarter, e.g. 2024Q4")
    args = parser.parse_args()

    config = load_config(args.config)
    setup_logging(config["paths"]["log_file"])

    start_year, start_qtr = config["run"]["start_year"], config["run"]["start_quarter"]
    end_year, end_qtr = config["run"]["end_year"], config["run"]["end_quarter"]
    if args.start_quarter:
        start_year, start_qtr = _parse_quarter_label(args.start_quarter)
    if args.end_quarter:
        end_year, end_qtr = _parse_quarter_label(args.end_quarter)

    checkpoint = load_checkpoint(config["paths"]["checkpoint_file"])
    completed = set(checkpoint.get("completed_quarters", []))

    logger.info("Loading universe from %s", config["universe"]["input_csv"])
    cik_set, cik_to_ticker = load_universe(config["universe"]["input_csv"])
    logger.info("%d CIKs in universe", len(cik_set))

    output_csv = config["paths"]["output_csv"]
    output_dir = os.path.dirname(output_csv)
    if output_dir:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
    header_written = os.path.exists(output_csv) and os.path.getsize(output_csv) > 0

    sleep_sec = config["sec_edgar"].get("request_sleep_sec", 0.5)

    for year, qtr in iter_quarters(start_year, start_qtr, end_year, end_qtr):
        label = f"{year}Q{qtr}"
        if label in completed:
            logger.info("%s already collected, skipping", label)
            continue

        try:
            zf = fetch_quarter_zip(config["sec_edgar"], year, qtr)
        except QuarterNotAvailable:
            logger.info("%s not yet published, skipping", label)
            completed.add(label)
            checkpoint["completed_quarters"] = sorted(completed)
            save_checkpoint(config["paths"]["checkpoint_file"], checkpoint)
            continue
        except Exception:
            logger.exception("%s failed to download — checkpoint preserved, rerun to retry", label)
            raise

        try:
            raw = collect_quarter(zf, cik_set)
            if raw is None:
                logger.info("%s: no matching filings for this universe", label)
            else:
                cleaned = process_quarter(raw, cik_to_ticker)
                cleaned.to_csv(output_csv, mode="a", header=not header_written, index=False)
                header_written = True
                logger.info("%s: appended %d rows", label, len(cleaned))
        except Exception:
            logger.exception("%s failed to process — checkpoint preserved, rerun to retry", label)
            raise

        completed.add(label)
        checkpoint["completed_quarters"] = sorted(completed)
        save_checkpoint(config["paths"]["checkpoint_file"], checkpoint)
        time.sleep(sleep_sec)

    logger.info("Pipeline completed. Output at %s", output_csv)


if __name__ == "__main__":
    main()
