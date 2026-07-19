"""
DAG 1: EODHD Price Ingestion Pipeline
=======================================
Fetches OHLCV price data from EODHD:
  - EOD (end-of-day) daily bars — available on all plans
  - Intraday (1m/5m) bars — requires EOD+Intraday plan (£29.99/mo)

Free tier: 20 API calls/day. Each EOD request = 1 call per ticker.
Demo mode: fetches daily bars for a few tickers.

Data flow:
  EODHD EOD/Intraday API → parse OHLCV → store in PostgreSQL price_data table

Production cadence (wired 2026-07-08 after Cycle 2 validation):
  * DAG schedule       : "0 4 * * 2-6"  (04:00 UTC, Tue–Sat)
  * fetch_eod_prices   : always runs; captures previous trading day's EOD bars
                         after full vendor propagation (~3–4h post-close).
                         Cost ≈ 503 credits per run.
  * fetch_intraday_prices : auto-skips via the market-hours gate at 04:00 UTC
                            (04:00 is not in 13:30–20:00 UTC), returning 0 rows
                            for zero credit cost. Intraday market-hours polling
                            is the responsibility of a companion DAG scheduled
                            "*/30 13-20 * * 1-5" (see project docs); no
                            functional overlap with this overnight run.

Rationale for "0 4 * * 2-6" over the earlier proposal "0 21 * * 1-5":
  Empirical observation (Cycle 2, 8 Jul 2026 20:53 UTC): only 129 of 503
  active tickers had their day's EOD bar available at T+53 min post-close.
  The vendor's EOD propagation converges within ~3–4 h of close. Scheduling
  at 04:00 UTC of the *following* calendar day (i.e. Tue–Sat, capturing
  Mon–Fri closes respectively) guarantees full-universe coverage without
  needing a same-day catch-up run. Weekends skipped naturally.

Currently `schedule=None` during Phase 1 validation. When the DAG is enabled
for production (post-Phase 2 lab bring-up), change to `"0 4 * * 2-6"`.
"""

from __future__ import annotations

import os
import logging
from datetime import datetime, timezone

import requests
import pendulum
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

EODHD_TOKEN = os.environ.get("EODHD_API_TOKEN", "")


@dag(
    dag_id="eodhd_price_pipeline",
    # Manual trigger during Phase 1 local validation.
    # Production schedule (wired 2026-07-08, activate post-Phase 2):
    #   EOD overnight run : "0 4 * * 2-6"  (04:00 UTC, Tue-Sat)
    #     -> fetch_eod_prices runs; fetch_intraday_prices auto-skips via
    #        market-hours gate (04:00 UTC not in 13:30-20:00 UTC).
    #     -> Cost: ~503 credits/day.
    #   Intraday market-hours run : delegated to a companion DAG scheduled
    #     "*/30 13-20 * * 1-5"  (see project docs / Chapter 4 §4.4 for rationale).
    #     -> Cost: ~42k credits/day.
    #   Total: ~63k credits/day on the paid 100k/day plan.
    # To go live: change `schedule=None` to `schedule="0 4 * * 2-6"` and
    # unpause the DAG via `airflow dags unpause eodhd_price_pipeline`.
    schedule="0 4 * * 2-6",
    # ^^ Overnight EOD run, Tue-Sat 04:00 UTC (fully-propagated bars).
    # DAG lands paused via AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true;
    # activate on the VM with: airflow dags unpause eodhd_price_pipeline
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs on scheduled cadence
    tags=["ingestion", "prices", "eodhd"],
    doc_md="""
    ### EODHD Price Pipeline
    Fetches OHLCV price data (EOD daily bars on free tier, intraday on paid).
    Stores in PostgreSQL `price_data` table with millisecond timestamps.

    **Production cadence (wired 2026-07-08):** `0 4 * * 2-6` (04:00 UTC, Tue-Sat).
    At this time `fetch_eod_prices` runs, capturing the previous trading day's
    fully-propagated bars; `fetch_intraday_prices` auto-skips via the market-hours
    gate. Intraday market-hours polling is handled by a companion DAG.

    **Runtime:** ~15 min on full S&P 500 (503 tickers × 1 credit EOD, or
    503 × 5 credits when the intraday task is not gated).
    """,
)
def eodhd_price_pipeline():

    @task
    def get_tickers() -> list[str]:
        """
        Return the list of currently active S&P 500 tickers from PostgreSQL.

        Single source of truth: the `companies` table is refreshed by
        sp500_refresh_dag (or manually via `scripts/fetch_sp500_constituents.py`).
        We only poll tickers with is_active=TRUE; removed constituents stop
        getting new data but their historical bars remain in price_data.

        On free EODHD tier (20 calls/day) this would burn the daily budget on
        ~20 tickers; on the paid tier (100K calls/day) you can poll all ~503.
        """
        from common.db_utils import get_active_sp500_tickers
        tickers = get_active_sp500_tickers()
        logger.info(f"Polling {len(tickers)} active S&P 500 tickers")
        return tickers

    @task
    def fetch_eod_prices(tickers: list[str]) -> int:
        """
        Fetch end-of-day OHLCV from EODHD and insert directly per-ticker.
        API: https://eodhd.com/api/eod/{TICKER}.US?api_token={TOKEN}&fmt=json&from=YYYY-MM-DD
        Each request = 1 API call. Returns total rows inserted.
        """
        # Per-ticker insert to avoid accumulating a giant in-memory list
        # (previous implementation returned ~40k dicts via XCom and OOM'd
        # on full-S&P-500 runs).
        from common.db_utils import insert_prices

        from_date = (datetime.now() - __import__("datetime").timedelta(days=30)).strftime("%Y-%m-%d")
        total_inserted = 0
        tickers_processed = 0
        tickers_ok = 0

        for ticker in tickers:
            url = (
                f"https://eodhd.com/api/eod/{ticker}.US"
                f"?api_token={EODHD_TOKEN}&fmt=json&from={from_date}&order=d"
            )
            tickers_processed += 1

            try:
                resp = requests.get(url, timeout=30)
                if resp.status_code != 200:
                    logger.error(f"EODHD returned {resp.status_code} for {ticker}: {resp.text[:200]}")
                    continue

                bars = resp.json()
                if not isinstance(bars, list):
                    logger.warning(f"Unexpected response for {ticker}: {type(bars)}")
                    continue

                # Build this ticker's rows, insert, then free memory.
                ticker_rows = []
                for bar in bars:
                    date_str = bar.get("date", "")
                    try:
                        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                        ts_ms = int(dt.timestamp() * 1000)
                    except ValueError:
                        continue

                    ticker_rows.append({
                        "ticker": ticker,
                        "timestamp_ms": ts_ms,
                        "datetime_utc": dt.isoformat(),
                        "open": bar.get("open"),
                        "high": bar.get("high"),
                        "low": bar.get("low"),
                        "close": bar.get("close"),
                        "volume": bar.get("volume"),
                        "interval": "1d",
                        "source": "eodhd",
                    })

                if ticker_rows:
                    inserted = insert_prices(ticker_rows)
                    total_inserted += inserted
                    tickers_ok += 1

                if tickers_processed % 50 == 0:
                    logger.info(
                        f"EOD progress: {tickers_processed}/{len(tickers)} tickers, "
                        f"{total_inserted} rows so far"
                    )

            except Exception as e:
                logger.error(f"Error fetching prices for {ticker}: {e}")
                continue

        logger.info(
            f"EOD complete: {tickers_ok}/{tickers_processed} tickers OK, "
            f"{total_inserted} rows inserted"
        )
        return total_inserted

    @task
    def fetch_intraday_prices(tickers: list[str]) -> int:
        """
        Fetch intraday OHLCV from EODHD (requires paid plan) and insert per-ticker.
        API: https://eodhd.com/api/intraday/{TICKER}.US?api_token={TOKEN}&interval=5m&fmt=json
        Each request = 5 API calls. Returns total rows inserted.

        Skips the fetch entirely if the US equity market is closed
        (weekends or outside 13:30-20:00 UTC on weekdays), because
        no new intraday data will exist and the call wastes credits.
        Set env var EODHD_IGNORE_MARKET_HOURS=true to bypass the gate.
        """
        # ---- Market-hours gate ----
        if os.environ.get("EODHD_IGNORE_MARKET_HOURS", "false").lower() != "true":
            now = datetime.now(timezone.utc)
            # US market: Mon-Fri (weekday 0-4), 13:30-20:00 UTC (09:30-16:00 ET; ignores DST)
            is_weekday = now.weekday() < 5
            hour_dec = now.hour + now.minute / 60.0
            is_market_hours = 13.5 <= hour_dec <= 20.0
            if not (is_weekday and is_market_hours):
                logger.info(
                    "Market closed (weekday=%s, hour=%.2f UTC); skipping intraday fetch. "
                    "Set EODHD_IGNORE_MARKET_HOURS=true to override.",
                    is_weekday, hour_dec,
                )
                return 0

        # Per-ticker insert to avoid XCom OOM (see fetch_eod_prices).
        from common.db_utils import insert_prices

        # Free-tier gate: cap at 2 tickers if EODHD_FREE_TIER=true.
        if os.environ.get("EODHD_FREE_TIER", "false").lower() == "true":
            tickers = tickers[:2]

        total_inserted = 0
        tickers_processed = 0
        tickers_ok = 0

        for ticker in tickers:
            url = (
                f"https://eodhd.com/api/intraday/{ticker}.US"
                f"?api_token={EODHD_TOKEN}&interval=5m&fmt=json"
            )
            tickers_processed += 1

            try:
                resp = requests.get(url, timeout=30)
                if resp.status_code != 200:
                    logger.warning(
                        f"Intraday not available for {ticker} (status {resp.status_code})."
                    )
                    continue

                bars = resp.json()
                if not isinstance(bars, list):
                    continue

                ticker_rows = []
                for bar in bars:
                    ts = bar.get("timestamp")
                    if not ts:
                        continue
                    ts_ms = int(ts) * 1000
                    dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)

                    ticker_rows.append({
                        "ticker": ticker,
                        "timestamp_ms": ts_ms,
                        "datetime_utc": dt.isoformat(),
                        "open": bar.get("open"),
                        "high": bar.get("high"),
                        "low": bar.get("low"),
                        "close": bar.get("close"),
                        "volume": bar.get("volume"),
                        "interval": "5m",
                        "source": "eodhd",
                    })

                if ticker_rows:
                    inserted = insert_prices(ticker_rows)
                    total_inserted += inserted
                    tickers_ok += 1

                if tickers_processed % 50 == 0:
                    logger.info(
                        f"Intraday progress: {tickers_processed}/{len(tickers)} tickers, "
                        f"{total_inserted} rows so far"
                    )

            except Exception as e:
                logger.warning(f"Intraday fetch failed for {ticker}: {e}")
                continue

        logger.info(
            f"Intraday complete: {tickers_ok}/{tickers_processed} tickers OK, "
            f"{total_inserted} rows inserted"
        )
        return total_inserted

    @task
    def report_summary(eod_inserted: int, intraday_inserted: int) -> dict:
        """
        Log a summary. Inserts already happened per-ticker inside the
        fetch tasks — this task is now a lightweight aggregator so we
        keep an XCom-visible artefact for run-status queries.
        """
        summary = {
            "eod_rows_inserted": eod_inserted,
            "intraday_rows_inserted": intraday_inserted,
            "total_inserted": eod_inserted + intraday_inserted,
        }
        logger.info(f"Price DAG summary: {summary}")
        return summary

    # DAG flow
    tickers = get_tickers()
    eod_count = fetch_eod_prices(tickers)
    intraday_count = fetch_intraday_prices(tickers)
    report_summary(eod_count, intraday_count)


eodhd_price_pipeline()
