"""
DAG (historical / disabled): EODHD News Deep-History Backfill
==============================================================
One-shot backfill of financial news articles from EODHD, paginating back
through the archive per ticker via the `offset` parameter until either
the API returns an empty page or a per-ticker safety cap is hit.

Design intent
-------------
* Runs ONCE, on the Azure VM only, after project handoff. Seeds a
  historical news corpus for RAs to query without re-billing.
* Left DISABLED (schedule=None, paused-on-create) so it cannot be
  triggered accidentally on the local dev environment.
* Uses the same MongoDB upsert-by-dedup_key pattern as the live news
  DAG so an interrupted run can safely resume by re-triggering.

Cost estimate
-------------
Each news request costs 5 credits and returns up to 1,000 articles.
EODHD's news archive typically goes back ~5-7 years per US equity ticker,
though depth varies by publisher. Expected cost per full-universe run:

    503 tickers x avg 10 pages x 5 credits = ~25,150 credits
    (worst case, ~50 pages/ticker for mega-caps) = ~125,750 credits

The paid tier is 100k/day. This means a single run may NEED TO BE SPLIT
across multiple days by narrowing the ticker set via the
`EODHD_HISTORICAL_TICKERS` env var, or by lowering the per-ticker page cap.

Environment variables
---------------------
    EODHD_HISTORICAL_TICKERS         comma-separated ticker list, defaults to ALL
    EODHD_HISTORICAL_NEWS_MAX_PAGES  max pages per ticker (safety cap), default 50
    EODHD_HISTORICAL_NEWS_PAGE_SIZE  articles per request (max 1000), default 1000
    EODHD_API_TOKEN                  paid account token (required)
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
    dag_id="eodhd_news_historical_pipeline",
    schedule=None,                 # DISABLED — manual one-shot only
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,  # extra safety belt: starts paused
    tags=["ingestion", "news", "eodhd", "historical", "disabled"],
    doc_md="""
    ### EODHD Historical News Backfill (DISABLED by default)
    One-shot deep-history news backfill via offset pagination. Intended for
    the Azure VM only, after project handoff. Cost can approach the full
    daily EODHD ceiling on a single run — set EODHD_HISTORICAL_TICKERS to
    a subset to spread the cost across multiple days.
    """,
)
def eodhd_news_historical_pipeline():

    @task
    def get_tickers() -> list[str]:
        """Ticker list from PostgreSQL, or a subset via env override."""
        override = os.environ.get("EODHD_HISTORICAL_TICKERS", "").strip()
        if override:
            tickers = [t.strip().upper() for t in override.split(",") if t.strip()]
            logger.info(f"Historical news backfill: {len(tickers)} tickers (env override)")
            return tickers

        from common.db_utils import get_postgres_conn
        conn = get_postgres_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT ticker FROM companies ORDER BY ticker")
                tickers = [row[0] for row in cur.fetchall()]
        finally:
            conn.close()
        logger.info(f"Historical news backfill: {len(tickers)} tickers (full universe)")
        return tickers

    @task
    def fetch_historical_news(tickers: list[str]) -> dict:
        """
        Paginate through EODHD's news archive per ticker.
        Stops per-ticker when a page returns fewer than page_size articles.
        """
        from common.db_utils import upsert_documents
        from common.constants import NEWS_COLLECTION

        max_pages = int(os.environ.get("EODHD_HISTORICAL_NEWS_MAX_PAGES", "50"))
        page_size = int(os.environ.get("EODHD_HISTORICAL_NEWS_PAGE_SIZE", "1000"))

        total_fetched = 0
        total_inserted = 0
        tickers_processed = 0

        for ticker in tickers:
            tickers_processed += 1
            ticker_articles: list[dict] = []
            offset = 0

            for page in range(max_pages):
                url = (
                    f"https://eodhd.com/api/news"
                    f"?s={ticker}.US&offset={offset}&limit={page_size}"
                    f"&api_token={EODHD_TOKEN}&fmt=json"
                )
                try:
                    resp = requests.get(url, timeout=60)
                    if resp.status_code != 200:
                        logger.warning(
                            f"News page failed for {ticker} offset={offset} "
                            f"(status {resp.status_code}): {resp.text[:200]}"
                        )
                        break

                    articles = resp.json()
                    if not isinstance(articles, list) or not articles:
                        break

                    for article in articles:
                        published = article.get("date", "")
                        title = article.get("title", "")
                        content = article.get("content", "")
                        sentiment = article.get("sentiment", {})
                        symbols = article.get("symbols", [])

                        ticker_articles.append({
                            "ticker": ticker,
                            "title": title,
                            "content": content,
                            "published_at": published,
                            "url": article.get("link", ""),
                            "symbols_mentioned": symbols,
                            "tags": article.get("tags", []),
                            "eodhd_sentiment": {
                                "polarity": sentiment.get("polarity"),
                                "neg": sentiment.get("neg"),
                                "neu": sentiment.get("neu"),
                                "pos": sentiment.get("pos"),
                            } if sentiment else None,
                            "dedup_key": f"{published}|{title[:100]}|{ticker}",
                            "ingested_at": datetime.now(timezone.utc).isoformat(),
                            "source": "eodhd_historical",
                        })

                    offset += len(articles)

                    # Partial page => we've hit the archive tail for this ticker
                    if len(articles) < page_size:
                        break

                except Exception as e:
                    logger.error(f"Historical news fetch error for {ticker} @offset={offset}: {e}")
                    break

            if ticker_articles:
                inserted = upsert_documents(NEWS_COLLECTION, ticker_articles, dedup_field="dedup_key")
                total_fetched += len(ticker_articles)
                total_inserted += inserted
                logger.info(
                    f"[{tickers_processed}/{len(tickers)}] {ticker}: "
                    f"{len(ticker_articles)} fetched / {inserted} inserted"
                )

        return {
            "tickers_processed": tickers_processed,
            "total_fetched": total_fetched,
            "total_inserted": total_inserted,
        }

    tickers = get_tickers()
    fetch_historical_news(tickers)


eodhd_news_historical_pipeline()
