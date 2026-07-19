"""
DAG (historical / disabled): EODHD Price Deep-History Backfill
================================================================
One-shot backfill of end-of-day (daily) OHLCV bars from EODHD, going back as
far as the API allows per ticker. Standard EODHD subscription covers back to
~2000-01-01 for US equities; some tickers begin later (IPO date).

Design intent
-------------
* Runs ONCE, on the Azure VM only, after the project is handed off. Intended
  to seed the /data folder with a deep historical corpus that Research
  Assistants (RAs) can query later without re-billing the paid EODHD account.
* Left DISABLED (schedule=None, DAG paused by default) so nobody accidentally
  triggers it on the local dev environment. A single manual trigger on the
  Azure VM is the only expected invocation.
* Uses the same per-ticker insert pattern as the live price DAG, and the same
  ON CONFLICT DO NOTHING semantics, so an interrupted or partial run can be
  safely resumed by re-triggering.

Cost estimate
-------------
EOD requests cost 1 credit each. One full-universe run = 503 credits. The
20-year window will land ~20 x 252 = ~5,040 rows per ticker, ~2.5M rows total.
The response payload is a single JSON array per ticker so the request count
is 1 per ticker regardless of history depth.

Intraday (5m) history is NOT included because EODHD's intraday endpoint only
serves the last ~120 days of data on the standard plan, which the live
intraday DAG already covers on an ongoing basis.

Environment variables
---------------------
    EODHD_HISTORICAL_FROM_DATE   ISO date, defaults to "2005-01-01"
                                 (~20 years back at time of writing)
    EODHD_HISTORICAL_TO_DATE     ISO date, defaults to today
    EODHD_API_TOKEN              paid account token (required)

Schedule stays None permanently. This DAG is a manual, one-shot operator tool.
"""

from __future__ import annotations

import os
import logging
from datetime import datetime, timezone, timedelta

import requests
import pendulum
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

EODHD_TOKEN = os.environ.get("EODHD_API_TOKEN", "")


@dag(
    dag_id="eodhd_price_historical_pipeline",
    schedule=None,             # DISABLED — manual one-shot only
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,   # extra safety belt: starts paused
    tags=["ingestion", "prices", "eodhd", "historical", "disabled"],
    doc_md="""
    ### EODHD Historical Price Backfill (DISABLED by default)
    One-shot deep-history EOD backfill. Intended for the Azure VM only, after
    project handoff. Do NOT unpause on the local dev environment.
    Cost: ~503 credits per run. Rows: ~2.5M for a 20-year window.
    """,
)
def eodhd_price_historical_pipeline():

    @task
    def get_tickers() -> list[str]:
        """Active + inactive tickers so historical rows land for removed constituents too."""
        from common.db_utils import get_postgres_conn
        conn = get_postgres_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT ticker FROM companies ORDER BY ticker")
                tickers = [row[0] for row in cur.fetchall()]
        finally:
            conn.close()
        logger.info(f"Historical price backfill: {len(tickers)} tickers "
                    f"(includes soft-deleted for survivorship-bias-free history)")
        return tickers

    @task
    def fetch_historical_eod(tickers: list[str]) -> int:
        """
        Fetch deep-history EOD bars per-ticker between EODHD_HISTORICAL_FROM_DATE
        and EODHD_HISTORICAL_TO_DATE.
        """
        from common.db_utils import insert_prices

        from_date = os.environ.get("EODHD_HISTORICAL_FROM_DATE", "2005-01-01")
        to_date = os.environ.get(
            "EODHD_HISTORICAL_TO_DATE",
            datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        )
        logger.info(f"Historical window: {from_date} -> {to_date}")

        total_inserted = 0
        tickers_processed = 0
        tickers_ok = 0

        for ticker in tickers:
            url = (
                f"https://eodhd.com/api/eod/{ticker}.US"
                f"?api_token={EODHD_TOKEN}&fmt=json"
                f"&from={from_date}&to={to_date}&order=a"
            )
            tickers_processed += 1

            try:
                resp = requests.get(url, timeout=60)
                if resp.status_code != 200:
                    logger.warning(
                        f"Historical EOD not available for {ticker} "
                        f"(status {resp.status_code}): {resp.text[:200]}"
                    )
                    continue

                bars = resp.json()
                if not isinstance(bars, list):
                    continue

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
                        "source": "eodhd_historical",
                    })

                if ticker_rows:
                    inserted = insert_prices(ticker_rows)
                    total_inserted += inserted
                    tickers_ok += 1

                if tickers_processed % 50 == 0:
                    logger.info(
                        f"Historical progress: {tickers_processed}/{len(tickers)} tickers, "
                        f"{total_inserted} rows so far"
                    )

            except Exception as e:
                logger.error(f"Historical fetch failed for {ticker}: {e}")
                continue

        logger.info(
            f"Historical EOD backfill complete: {tickers_ok}/{tickers_processed} "
            f"tickers OK, {total_inserted} rows inserted"
        )
        return total_inserted

    tickers = get_tickers()
    fetch_historical_eod(tickers)


eodhd_price_historical_pipeline()
