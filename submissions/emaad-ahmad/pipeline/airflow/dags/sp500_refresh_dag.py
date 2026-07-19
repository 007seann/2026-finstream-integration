"""
DAG 5: S&P 500 Constituent Auto-Refresh
==========================================
Keeps the `companies` table in sync with the live S&P 500 index.

S&P 500 constituents change a few times per year (additions, deletions). This
DAG runs weekly, fetches the current constituent list from Wikipedia, and
reconciles the `companies` table:

  Tickers in Wikipedia but NOT in DB      → INSERT  (is_active=TRUE,  added_at=NOW())
  Tickers in DB but NOT in Wikipedia      → UPDATE  is_active=FALSE,  removed_at=NOW()
  Tickers in both (existing constituents) → UPDATE  name/sector/cik if changed

Tickers are NEVER deleted — preserves historical price/news/sentiment data
for companies that left the index. Downstream DAGs only poll `is_active=TRUE`,
so deactivated tickers stop receiving new data automatically.

Cost: free (Wikipedia, no API key).
Cadence: weekly. The index doesn't change often enough to warrant daily polling.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import pendulum
import requests
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
HTTP_TIMEOUT_S = 60


@dag(
    dag_id="sp500_refresh_pipeline",
    schedule="0 4 * * 1",          # Every Monday 04:00 UTC
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=10),
    },
    tags=["maintenance", "sp500", "companies"],
    doc_md="""
    ### S&P 500 Constituent Auto-Refresh
    Reconciles the `companies` table against the live S&P 500 index every Monday
    at 04:00 UTC. Adds new constituents, deactivates removed ones, updates
    metadata. Never deletes — preserves history.
    """,
)
def sp500_refresh_pipeline():

    @task
    def fetch_constituents_from_wikipedia() -> list[dict]:
        """Parse the constituents table from Wikipedia's `List_of_S&P_500_companies`."""
        headers = {
            "User-Agent": "MSc-IPP-Dissertation-Research/1.0 "
                          "(University of Edinburgh, contact: ahmad.emaad19@gmail.com)"
        }
        resp = requests.get(WIKI_URL, headers=headers, timeout=HTTP_TIMEOUT_S)
        resp.raise_for_status()

        import io
        import pandas as pd

        tables = pd.read_html(io.StringIO(resp.text))
        df = tables[0].rename(columns={
            "Symbol":      "ticker",
            "Security":    "company_name",
            "GICS Sector": "sector",
            "CIK":         "cik",
        })

        rows = []
        for _, r in df.iterrows():
            ticker = str(r.get("ticker", "")).strip().replace(".", "-")
            if not ticker:
                continue
            cik = r.get("cik")
            rows.append({
                "ticker":       ticker,
                "company_name": str(r.get("company_name", "")).strip(),
                "sector":       str(r.get("sector", "")).strip() or None,
                "cik":          str(cik).strip().zfill(10) if cik else None,
            })

        logger.info(f"Wikipedia returned {len(rows)} S&P 500 constituents")
        return rows

    @task
    def reconcile_companies_table(rows: list[dict]) -> dict:
        """Three-way merge of fetched rows against `companies` table."""
        if not rows:
            raise ValueError("Empty constituent list — refusing to touch DB")

        from common.db_utils import get_postgres_conn

        fetched_tickers = {r["ticker"] for r in rows}
        conn = get_postgres_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT ticker, is_active FROM companies")
                db_state = {row[0]: row[1] for row in cur.fetchall()}
                db_tickers = set(db_state.keys())

                # Upsert all fetched rows as active
                for r in rows:
                    cur.execute(
                        """
                        INSERT INTO companies (ticker, company_name, sector, cik, is_active, added_at)
                        VALUES (%s, %s, %s, %s, TRUE, NOW())
                        ON CONFLICT (ticker) DO UPDATE SET
                            company_name = EXCLUDED.company_name,
                            sector       = EXCLUDED.sector,
                            cik          = EXCLUDED.cik,
                            is_active    = TRUE,
                            removed_at   = NULL,
                            updated_at   = NOW()
                        """,
                        (r["ticker"], r["company_name"], r.get("sector"), r.get("cik")),
                    )

                # Deactivate any DB ticker no longer in the index
                removed = db_tickers - fetched_tickers
                if removed:
                    cur.execute(
                        """
                        UPDATE companies
                           SET is_active  = FALSE,
                               removed_at = NOW(),
                               updated_at = NOW()
                         WHERE ticker = ANY(%s)
                           AND is_active = TRUE
                        """,
                        (list(removed),),
                    )

            conn.commit()
        finally:
            conn.close()

        added = sorted(fetched_tickers - db_tickers)
        reactivated = sorted(
            t for t in fetched_tickers
            if t in db_state and db_state[t] is False
        )
        deactivated = sorted(db_tickers - fetched_tickers)

        result = {
            "checked_at":        datetime.now(timezone.utc).isoformat(),
            "fetched_count":     len(rows),
            "db_count_before":   len(db_tickers),
            "added":             added,
            "reactivated":       reactivated,
            "deactivated":       deactivated,
        }
        logger.info(
            "S&P 500 reconcile: +%d added | +%d reactivated | -%d deactivated",
            len(added), len(reactivated), len(deactivated),
        )
        if added:
            logger.info(f"Added tickers: {added}")
        if deactivated:
            logger.info(f"Deactivated tickers: {deactivated}")
        return result

    rows = fetch_constituents_from_wikipedia()
    reconcile_companies_table(rows)


sp500_refresh_pipeline()
