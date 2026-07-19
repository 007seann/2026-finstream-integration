"""
DAG 1b: EODHD Intraday Price Ingestion Pipeline (companion to eodhd_price_dag.py)
==================================================================================
Companion to the overnight EOD DAG. This DAG owns *intraday* 5-minute polling
during US market hours only. The overnight EOD DAG (eodhd_price_pipeline)
handles daily bars after full vendor propagation; the two together give:

  overnight EOD run     : 0 4 * * 2-6   (fully-propagated daily bars, ~503 credits)
  intraday market hours : */30 13-20 * * 1-5   (5-min bars, ~2515 credits per run)

Cost at production cadence:
  * 14 intraday polls / day x 2,515 credits = ~35,210 credits/day
  * combined with EOD (~500/day) and news (~20k/day) = ~55k/day, ~55% of the
    100,000/day paid ceiling. Leaves 45% headroom for Sarah's fundamentals
    ingestion in Phase 3 and the historical backfill DAGs.

Same market-hours gate as the original combined DAG. Same insert_prices helper.
Same ON CONFLICT (ticker, timestamp_ms, interval) DO NOTHING semantics for
idempotency. Same per-ticker streaming pattern to keep XCom payload bounded.

Schedule stays None during Phase 1 validation. Post-Phase-2 activation:
    schedule = "*/30 13-20 * * 1-5"
    airflow dags unpause eodhd_intraday_pipeline
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
    dag_id="eodhd_intraday_pipeline",
    # Production schedule proposal (post-Phase 2):
    #   "*/30 13-20 * * 1-5"   every 30 min, market hours, weekdays
    # Kept None during validation; unpause explicitly after Phase 2 lab bring-up.
    schedule="*/30 13-20 * * 1-5",
    # ^^ Every 30 min during US market hours (13:30-20:00 UTC), weekdays only.
    # Market-hours gate inside fetch_intraday_prices short-circuits any
    # off-hours triggers to save credits. DAG lands paused via
    # AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true; activate on the VM
    # with: airflow dags unpause eodhd_intraday_pipeline
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "prices", "eodhd", "intraday"],
    doc_md="""
    ### EODHD Intraday Price Pipeline (companion)
    Splits intraday 5-minute polling out of the combined price DAG so that the
    overnight EOD DAG can run on a different cron schedule.
    Runtime: ~15 min on full S&P 500. Cost: ~2,515 credits per run.
    """,
)
def eodhd_intraday_pipeline():

    @task
    def get_tickers() -> list[str]:
        """Active S&P 500 tickers from PostgreSQL (single source of truth)."""
        from common.db_utils import get_active_sp500_tickers
        tickers = get_active_sp500_tickers()
        logger.info(f"Polling {len(tickers)} active S&P 500 tickers")
        return tickers

    @task
    def fetch_intraday_prices(tickers: list[str]) -> int:
        """
        Fetch 5-minute OHLCV per-ticker. Short-circuits outside US market hours
        so scheduled off-hours triggers cost zero credits.
        """
        # ---- Market-hours gate ----
        if os.environ.get("EODHD_IGNORE_MARKET_HOURS", "false").lower() != "true":
            now = datetime.now(timezone.utc)
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

        from common.db_utils import insert_prices

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
                    logger.warning(f"Intraday not available for {ticker} (status {resp.status_code}).")
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

    tickers = get_tickers()
    fetch_intraday_prices(tickers)


eodhd_intraday_pipeline()
