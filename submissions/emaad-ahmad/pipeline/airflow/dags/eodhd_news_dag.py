"""
DAG 2: EODHD Financial News Ingestion Pipeline
================================================
Fetches financial news articles from EODHD API (ticker-tagged, with sentiment).
Free tier: 20 API calls/day (each news request = 5 API calls per ticker).
So free tier = ~4 ticker lookups per day. Demo mode fetches top tickers only.

Data flow:
  EODHD News API → parse articles → store in MongoDB news_articles collection

Schedule: Manual trigger (in production: every 15 minutes)
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
    dag_id="eodhd_news_pipeline",
    # Production cadence: "0 */3 * * *" (every 3 hours, 24/7).
    # Cost: ~20k credits/day (8 runs x ~2,515 credits per full-universe poll).
    # DAG lands paused via AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true;
    # activate on the VM with: airflow dags unpause eodhd_news_pipeline
    schedule="0 */3 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs on scheduled cadence
    tags=["ingestion", "news", "eodhd"],
    doc_md="""
    ### EODHD News Pipeline
    Fetches financial news from EODHD API.
    **Free tier**: 20 calls/day (~4 tickers). Paid: 100K calls/day.
    Articles come pre-tagged with ticker symbols, reducing NER burden.
    Runtime: ~15-18 min on full S&P 500 (503 tickers x 5 credits).
    """,
)
def eodhd_news_pipeline():

    @task
    def get_tickers() -> list[str]:
        """
        Return active S&P 500 tickers from PostgreSQL.

        EODHD news API costs 5 credits per ticker request.
          - Free tier (20 credits/day) → only 4 tickers can be polled per day.
            On free tier we fall back to a fixed 4-ticker rotation for cost reasons.
          - Paid tier (100K credits/day) → all ~503 active tickers can be polled
            every 15 minutes with headroom.

        EODHD_FREE_TIER env var ('true' / 'false') controls the rotation.
        """
        import os
        from common.db_utils import get_active_sp500_tickers
        all_tickers = get_active_sp500_tickers()

        if os.environ.get("EODHD_FREE_TIER", "false").lower() == "true":
            # Free tier: cap at 4 tickers to stay within 20 credits/day budget
            limited = all_tickers[:4]
            logger.info(f"Free-tier mode: polling {len(limited)} of {len(all_tickers)} tickers")
            return limited

        logger.info(f"Polling {len(all_tickers)} active S&P 500 tickers")
        return all_tickers

    @task
    def fetch_news(tickers: list[str]) -> list[dict]:
        """
        Fetch recent news for each ticker from EODHD.
        API: https://eodhd.com/api/news?s={TICKER}.US&api_token={TOKEN}&fmt=json
        Each request costs 5 API calls.
        """
        all_articles = []

        for ticker in tickers:
            url = f"https://eodhd.com/api/news?s={ticker}.US&offset=0&limit=10&api_token={EODHD_TOKEN}&fmt=json"
            logger.info(f"Fetching news for {ticker}")

            try:
                resp = requests.get(url, timeout=30)
                if resp.status_code != 200:
                    logger.error(f"EODHD returned {resp.status_code} for {ticker}: {resp.text[:200]}")
                    continue

                articles = resp.json()
                if not isinstance(articles, list):
                    logger.warning(f"Unexpected response type for {ticker}: {type(articles)}")
                    continue

                for article in articles:
                    # EODHD returns: date, title, content, link, symbols, tags, sentiment
                    published = article.get("date", "")
                    title = article.get("title", "")
                    content = article.get("content", "")

                    # Extract sentiment if provided by EODHD
                    sentiment = article.get("sentiment", {})

                    # Tickers mentioned in this article (EODHD provides this!)
                    symbols = article.get("symbols", [])

                    doc = {
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
                        "source": "eodhd",
                    }
                    all_articles.append(doc)

                logger.info(f"  {ticker}: fetched {len(articles)} articles")

            except Exception as e:
                logger.error(f"Error fetching news for {ticker}: {e}")
                continue

        logger.info(f"Total articles collected: {len(all_articles)}")
        return all_articles

    @task
    def store_news(articles: list[dict]) -> dict:
        """Store articles in MongoDB news_articles collection."""
        from common.db_utils import upsert_documents
        from common.constants import NEWS_COLLECTION

        inserted = upsert_documents(NEWS_COLLECTION, articles, dedup_field="dedup_key")
        return {"total": len(articles), "inserted": inserted}

    # DAG flow
    tickers = get_tickers()
    articles = fetch_news(tickers)
    result = store_news(articles)


eodhd_news_pipeline()
