"""
Sentiment Enrichment DAG (RQ3)
================================
Runs FinBERT and RoBERTa sentiment inference on news articles,
compares with EODHD's pre-scored sentiment, and stores results.

Pipeline:
  1. Fetch unscored articles from MongoDB
  2. Score with FinBERT (primary)
  3. Score with RoBERTa (secondary / cross-validation)
  4. Store sentiment_scores in MongoDB
  5. Enrich fused_events with sentiment data

Note: First run downloads models (~400MB each). Subsequent runs
use the cached models via HF_HOME volume mount.
"""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.decorators import task

from common.constants import (
    NEWS_COLLECTION,
    SENTIMENT_COLLECTION,
    FUSED_EVENTS_COLLECTION,
    FINBERT_MODEL,
    ROBERTA_MODEL,
)
from common.db_utils import get_mongo_client, upsert_documents

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="sentiment_enrichment_pipeline",
    default_args=default_args,
    description="RQ3: FinBERT/RoBERTa sentiment scoring + EODHD comparison",
    # Production cadence: "30 * * * *" (hourly, offset 30 min from fusion so
    # it runs on freshly-fused events). No EODHD cost; local transformer
    # inference only. DAG lands paused via
    # AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true; activate on the VM
    # with: airflow dags unpause sentiment_enrichment_pipeline
    schedule="30 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs on scheduled cadence
    tags=["rq3", "sentiment", "finbert", "roberta"],
) as dag:

    @task()
    def get_unscored_articles():
        """Find news articles that haven't been scored yet."""
        client, db = get_mongo_client()
        try:
            # Get dedup_keys of already-scored articles (FinBERT)
            scored_keys = set()
            cursor = db[SENTIMENT_COLLECTION].find(
                {"model": FINBERT_MODEL},
                {"source_dedup_key": 1, "_id": 0},
            )
            for doc in cursor:
                scored_keys.add(doc.get("source_dedup_key"))

            # Get all news articles not yet scored
            all_articles = list(db[NEWS_COLLECTION].find(
                {},
                {"_id": 0},
            ))

            unscored = [a for a in all_articles if a.get("dedup_key") not in scored_keys]
            print(f"Found {len(unscored)} unscored articles (out of {len(all_articles)} total)")
            return unscored
        finally:
            client.close()

    @task()
    def score_with_finbert(articles: list):
        """Score articles with FinBERT (primary model)."""
        from packages.sentiment.finbert_scorer import score_article

        if not articles:
            print("No articles to score")
            return []

        print(f"Scoring {len(articles)} articles with FinBERT...")
        all_scores = []
        for i, article in enumerate(articles):
            scores = score_article(article, models=[FINBERT_MODEL])
            all_scores.extend(scores)
            if (i + 1) % 10 == 0:
                print(f"  Scored {i + 1}/{len(articles)}")

        print(f"FinBERT: {len(all_scores)} scores generated")

        # Quick stats
        if all_scores:
            labels = [s["label"] for s in all_scores]
            print(f"  Positive: {labels.count('positive')}, "
                  f"Negative: {labels.count('negative')}, "
                  f"Neutral: {labels.count('neutral')}")
            agreements = [s.get("eodhd_comparison", {}).get("agreement") for s in all_scores]
            agreed = sum(1 for a in agreements if a is True)
            total = sum(1 for a in agreements if a is not None)
            if total:
                print(f"  Agreement with EODHD: {agreed}/{total} ({100*agreed/total:.1f}%)")

        return all_scores

    @task()
    def score_with_roberta(articles: list):
        """Score articles with DistilRoBERTa-financial (secondary model)."""
        from packages.sentiment.finbert_scorer import score_article

        if not articles:
            print("No articles to score")
            return []

        print(f"Scoring {len(articles)} articles with RoBERTa...")
        all_scores = []
        for i, article in enumerate(articles):
            scores = score_article(article, models=[ROBERTA_MODEL])
            all_scores.extend(scores)
            if (i + 1) % 10 == 0:
                print(f"  Scored {i + 1}/{len(articles)}")

        print(f"RoBERTa: {len(all_scores)} scores generated")

        if all_scores:
            labels = [s["label"] for s in all_scores]
            print(f"  Positive: {labels.count('positive')}, "
                  f"Negative: {labels.count('negative')}, "
                  f"Neutral: {labels.count('neutral')}")

        return all_scores

    @task()
    def store_scores(finbert_scores: list, roberta_scores: list):
        """Store all sentiment scores in MongoDB."""
        all_scores = finbert_scores + roberta_scores
        if not all_scores:
            print("No scores to store")
            return {"total": 0, "inserted": 0}

        inserted = upsert_documents(SENTIMENT_COLLECTION, all_scores, dedup_field="dedup_key")
        print(f"Stored {inserted}/{len(all_scores)} sentiment scores")
        return {"total": len(all_scores), "inserted": inserted}

    @task()
    def enrich_fused_events(finbert_scores: list, roberta_scores: list):
        """
        Enrich existing fused_events with transformer sentiment.
        Adds finbert_sentiment and roberta_sentiment to each
        news_context item in fused events.
        """
        if not finbert_scores and not roberta_scores:
            print("No scores to enrich with")
            return 0

        # Build lookup: source_dedup_key -> {finbert: {...}, roberta: {...}}
        sentiment_lookup = {}
        for score in finbert_scores:
            key = score.get("source_dedup_key")
            if key:
                if key not in sentiment_lookup:
                    sentiment_lookup[key] = {}
                sentiment_lookup[key]["finbert"] = {
                    "label": score["label"],
                    "confidence": score["confidence"],
                }
        for score in roberta_scores:
            key = score.get("source_dedup_key")
            if key:
                if key not in sentiment_lookup:
                    sentiment_lookup[key] = {}
                sentiment_lookup[key]["roberta"] = {
                    "label": score["label"],
                    "confidence": score["confidence"],
                }

        print(f"Enriching fused events with {len(sentiment_lookup)} sentiment scores...")

        client, db = get_mongo_client()
        try:
            fused_coll = db[FUSED_EVENTS_COLLECTION]
            updated = 0

            for fused_event in fused_coll.find({}):
                needs_update = False
                news_context = fused_event.get("news_context", [])

                for i, ctx in enumerate(news_context):
                    # Match by title (since news_context doesn't store dedup_key)
                    title = ctx.get("title", "")
                    # Find matching sentiment by checking all scored articles
                    for src_key, sentiments in sentiment_lookup.items():
                        # The source_dedup_key format: "{published_at}|{title[:100]}|{ticker}"
                        if title and title[:80] in src_key:
                            if "finbert" in sentiments and "finbert_sentiment" not in ctx:
                                ctx["finbert_sentiment"] = sentiments["finbert"]
                                needs_update = True
                            if "roberta" in sentiments and "roberta_sentiment" not in ctx:
                                ctx["roberta_sentiment"] = sentiments["roberta"]
                                needs_update = True
                            break

                if needs_update:
                    fused_coll.update_one(
                        {"_id": fused_event["_id"]},
                        {"$set": {"news_context": news_context}},
                    )
                    updated += 1

            print(f"Enriched {updated} fused events with sentiment data")
            return updated
        finally:
            client.close()

    # DAG flow
    articles = get_unscored_articles()
    fb_scores = score_with_finbert(articles)
    rb_scores = score_with_roberta(articles)
    store_scores(fb_scores, rb_scores)
    enrich_fused_events(fb_scores, rb_scores)
