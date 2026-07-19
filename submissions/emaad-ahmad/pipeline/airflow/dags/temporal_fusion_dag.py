"""
Temporal Fusion DAG (RQ2)
==========================
Implements the novel temporal fusion algorithm:
  1. Load price data from PostgreSQL
  2. Load news articles from MongoDB
  3. Entity mapping: spaCy NER -> filter noise (Benzinga boilerplate)
  4. Sliding window: align news to price ticks
  5. Store fused events in MongoDB

Configurable for daily (demo) and intraday (production):
  - Daily bars: 24h lookback window, 0 lookahead
  - Intraday:   30min lookback, 5min lookahead (IPP spec)
  Change by updating the price data interval.
"""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.decorators import task

from common.constants import (
    NEWS_COLLECTION,
    FUSED_EVENTS_COLLECTION,
)
from common.db_utils import (
    get_postgres_conn,
    get_mongo_client,
    upsert_documents,
    get_active_sp500_tickers,
)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="temporal_fusion_pipeline",
    default_args=default_args,
    description="RQ2: Entity mapping + temporal fusion (price-news alignment)",
    # Production cadence: "@hourly" (runtime ~28 min on full S&P 500 5m corpus).
    # No EODHD cost; reads from PostgreSQL and MongoDB only.
    # DAG lands paused via AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true;
    # activate on the VM with: airflow dags unpause temporal_fusion_pipeline
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs on scheduled cadence
    tags=["rq2", "fusion", "entity-mapping"],
) as dag:

    @task()
    def get_tickers() -> list:
        """
        Return the list of currently active S&P 500 tickers (from companies table).
        Replaces the previous load_prices which OOM'd on the full 5-min corpus.
        Prices are now fetched per-ticker inside run_fusion.
        """
        tickers = get_active_sp500_tickers()
        print(f"Fusion will iterate {len(tickers)} active S&P 500 tickers")
        return tickers

    def _load_prices_for_ticker(ticker: str) -> list:
        """Fetch price rows for a single ticker (avoids loading the whole corpus)."""
        conn = get_postgres_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ticker, datetime_utc, open, high, low, close, volume, interval
                    FROM price_data
                    WHERE ticker = %s
                    ORDER BY datetime_utc DESC
                    """,
                    (ticker,),
                )
                columns = [desc[0] for desc in cur.description]
                rows = []
                for row in cur.fetchall():
                    d = dict(zip(columns, row))
                    if isinstance(d["datetime_utc"], datetime):
                        d["datetime_utc"] = d["datetime_utc"].isoformat()
                    for key in ["open", "high", "low", "close"]:
                        if d[key] is not None:
                            d[key] = float(d[key])
                    rows.append(d)
                return rows
        finally:
            conn.close()

    @task()
    def load_news():
        """Load all news articles from MongoDB."""
        client, db = get_mongo_client()
        try:
            articles = list(db[NEWS_COLLECTION].find(
                {},
                {"_id": 0},  # Exclude MongoDB ObjectId (not serializable)
            ))
            print(f"Loaded {len(articles)} news articles")

            # Group by EODHD ticker tag
            by_ticker = {}
            for art in articles:
                ticker = art.get("ticker", "")
                if ticker not in by_ticker:
                    by_ticker[ticker] = []
                by_ticker[ticker].append(art)

            return by_ticker
        finally:
            client.close()

    @task()
    def load_companies():
        """Load company name -> ticker mapping from PostgreSQL."""
        conn = get_postgres_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT ticker, company_name FROM companies")
                columns = [desc[0] for desc in cur.description]
                rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            print(f"Loaded {len(rows)} companies for entity mapping")
            return rows
        finally:
            conn.close()

    @task()
    def run_entity_mapping(news_by_ticker: dict, companies: list):
        """
        Step 1 (RQ2): Entity mapping using spaCy NER.
        Filters out noisy ticker-article associations.
        """
        from packages.fusion.entity_mapper import build_company_lookup, map_article

        company_lookup = build_company_lookup(companies)
        print(f"Built company lookup with {len(company_lookup)} name variants")

        # Map every article
        all_articles = []
        for ticker, articles in news_by_ticker.items():
            all_articles.extend(articles)

        # Deduplicate by dedup_key
        seen = set()
        unique_articles = []
        for art in all_articles:
            dk = art.get("dedup_key")
            if dk and dk not in seen:
                seen.add(dk)
                unique_articles.append(art)

        print(f"Running entity mapping on {len(unique_articles)} unique articles...")

        entity_results = {}
        for art in unique_articles:
            result = map_article(art, company_lookup)
            dedup_key = art.get("dedup_key")
            if dedup_key:
                entity_results[dedup_key] = result

        # Stats
        total_relevant = sum(
            len(r["relevant_tickers"]) for r in entity_results.values()
        )
        articles_with_tickers = sum(
            1 for r in entity_results.values() if r["relevant_tickers"]
        )
        print(f"Entity mapping complete: {articles_with_tickers}/{len(unique_articles)} "
              f"articles have relevant tickers ({total_relevant} total associations)")

        return entity_results

    @task()
    def run_fusion(
        tickers: list,
        news_by_ticker: dict,
        entity_results: dict,
    ) -> dict:
        """
        Streaming per-ticker sliding-window temporal fusion.
        For each active S&P 500 ticker:
          1. Query prices from PostgreSQL for that ticker only.
          2. Combine originally-tagged news + entity-mapped news for that ticker.
          3. Fuse.
          4. Upsert into MongoDB immediately.
          5. Free memory before moving to the next ticker.
        Returns only counters (no giant XCom payload).

        Fixes both prior bugs:
          - Previous load_prices returned all 3.2M rows to XCom (OOM).
          - Previous run_fusion only iterated DEMO_TICKERS (top 10),
            so 490+ S&P 500 tickers were never fused.
        """
        from packages.fusion.temporal_fusion import fuse_ticker

        total_events = 0
        total_events_with_news = 0
        total_inserted = 0
        tickers_processed = 0
        tickers_with_prices = 0

        for ticker in tickers:
            tickers_processed += 1

            # Load THIS ticker's price rows only (typically 6k-8k rows)
            price_rows = _load_prices_for_ticker(ticker)
            if not price_rows:
                continue
            tickers_with_prices += 1

            # Combine news pool for this ticker:
            #   (a) articles originally tagged with this ticker by EODHD/GDELT
            #   (b) articles that entity mapping says are relevant to this ticker
            seen_keys = set()
            combined_news = []

            for art in news_by_ticker.get(ticker, []):
                dk = art.get("dedup_key")
                if dk and dk not in seen_keys:
                    seen_keys.add(dk)
                    combined_news.append(art)

            for other_ticker, other_articles in news_by_ticker.items():
                if other_ticker == ticker:
                    continue
                for art in other_articles:
                    dk = art.get("dedup_key")
                    if not dk or dk in seen_keys:
                        continue
                    mapping = entity_results.get(dk)
                    if not mapping:
                        continue
                    if any(t["ticker"] == ticker for t in mapping.get("relevant_tickers", [])):
                        seen_keys.add(dk)
                        combined_news.append(art)

            # Fuse and store immediately
            fused_events = fuse_ticker(ticker, price_rows, combined_news, entity_results)
            if fused_events:
                inserted = upsert_documents(
                    FUSED_EVENTS_COLLECTION,
                    fused_events,
                    dedup_field="dedup_key",
                )
                total_events += len(fused_events)
                total_events_with_news += sum(1 for e in fused_events if e["news_count"] > 0)
                total_inserted += inserted

            # Free memory before next iteration
            del price_rows
            del combined_news
            del fused_events

            if tickers_processed % 50 == 0:
                print(
                    f"Fusion progress: {tickers_processed}/{len(tickers)} tickers, "
                    f"{total_events} events, {total_inserted} inserted"
                )

        summary = {
            "tickers_processed":     tickers_processed,
            "tickers_with_prices":   tickers_with_prices,
            "total_events":          total_events,
            "events_with_news":      total_events_with_news,
            "total_inserted":        total_inserted,
        }
        print(f"Fusion complete: {summary}")
        return summary

    # DAG flow (load_prices task removed — prices are now fetched per-ticker
    # inside run_fusion to avoid the previous OOM on the full 5-min corpus)
    tickers = get_tickers()
    news = load_news()
    companies = load_companies()
    entity_map = run_entity_mapping(news, companies)
    run_fusion(tickers, news, entity_map)
