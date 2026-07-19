"""
FastAPI REST Query Layer
=========================
Exposes endpoints for retrieving financial data from the platform.
Matches the IPP §3.3 spec: OHLCV ticks, sentiment-annotated news, fused event lookups.

Endpoints:
  GET /                          — health check
  GET /v1/prices                 — OHLCV price data from PostgreSQL
  GET /v1/news                   — news articles from MongoDB
  GET /v1/fused                  — fused events (price + news context)
  GET /v1/stats                  — pipeline statistics

Disabled endpoints (kept in source for reference):
  GET /v1/filings                — SEC EDGAR filings (disabled 2026-05-28)
"""

import os
from datetime import datetime, timezone
from typing import Optional

import requests as http_requests
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI(
    title="Financial Data Platform API",
    description="REST layer for heterogeneous real-time financial data",
    version="0.1.0",
)

# Allow browser-based dashboards to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================
# Database connections
# =============================================================

def get_postgres_conn():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        user=os.environ.get("POSTGRES_USER", "finplatform"),
        password=os.environ.get("POSTGRES_PASSWORD", "finplatform_dev_2026"),
        dbname=os.environ.get("POSTGRES_DB", "financial_data"),
        cursor_factory=RealDictCursor,
    )


def get_mongo_db():
    host = os.environ.get("MONGO_HOST", "mongodb")
    port = int(os.environ.get("MONGO_PORT", 27017))
    user = os.environ.get("MONGO_INITDB_ROOT_USERNAME", "finplatform")
    pwd = os.environ.get("MONGO_INITDB_ROOT_PASSWORD", "finplatform_dev_2026")
    db_name = os.environ.get("MONGO_DB", "financial_db")
    client = MongoClient(f"mongodb://{user}:{pwd}@{host}:{port}/")
    return client[db_name]


# =============================================================
# Health check
# =============================================================

@app.get("/")
def root():
    return {"status": "ok", "service": "Financial Data Platform API", "version": "0.1.0"}


@app.get("/health")
def health():
    """Check connectivity to both databases."""
    status = {"postgres": "unknown", "mongodb": "unknown"}
    try:
        conn = get_postgres_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        status["postgres"] = "healthy"
    except Exception as e:
        status["postgres"] = f"error: {e}"

    try:
        db = get_mongo_db()
        db.command("ping")
        status["mongodb"] = "healthy"
    except Exception as e:
        status["mongodb"] = f"error: {e}"

    return status


# =============================================================
# GET /v1/prices — OHLCV from PostgreSQL
# =============================================================

@app.get("/v1/prices")
def get_prices(
    ticker: str = Query(..., description="Ticker symbol, e.g. AAPL"),
    start: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    interval: Optional[str] = Query("1d", description="Bar interval: 1m, 5m, 15m, 1h, 1d"),
    limit: int = Query(100, ge=1, le=5000),
):
    """Retrieve OHLCV price data for a ticker."""
    conn = get_postgres_conn()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT ticker, datetime_utc, open, high, low, close, volume, interval
                FROM price_data
                WHERE ticker = %s AND interval = %s
            """
            params = [ticker.upper(), interval]

            if start:
                query += " AND datetime_utc >= %s"
                params.append(start)
            if end:
                query += " AND datetime_utc <= %s"
                params.append(end)

            query += " ORDER BY datetime_utc DESC LIMIT %s"
            params.append(limit)

            cur.execute(query, params)
            rows = cur.fetchall()

        return {"ticker": ticker, "interval": interval, "count": len(rows), "data": rows}
    finally:
        conn.close()


# =============================================================
# GET /v1/news — Articles from MongoDB
# =============================================================

@app.get("/v1/news")
def get_news(
    ticker: str = Query(..., description="Ticker symbol"),
    limit: int = Query(20, ge=1, le=500),
):
    """Retrieve news articles for a ticker."""
    db = get_mongo_db()
    cursor = db.news_articles.find(
        {"ticker": ticker.upper()},
        {"_id": 0, "dedup_key": 0},
    ).sort("published_at", -1).limit(limit)

    articles = list(cursor)
    return {"ticker": ticker, "count": len(articles), "articles": articles}


# =============================================================
# GET /v1/filings — SEC filings from MongoDB
# [DISABLED 2026-05-28] SEC EDGAR out of scope. Endpoint kept for
# reference; remove the `if False:` guard to re-enable.
# =============================================================

if False:  # [DISABLED] SEC filings endpoint
    @app.get("/v1/filings")
    def get_filings(
        ticker: str = Query(..., description="Ticker symbol"),
        form_type: Optional[str] = Query(None, description="10-K or 10-Q"),
        limit: int = Query(20, ge=1, le=200),
    ):
        """Retrieve SEC filings for a ticker."""
        db = get_mongo_db()
        query = {"ticker": ticker.upper()}
        if form_type:
            query["form_type"] = form_type

        cursor = db.sec_filings.find(
            query,
            {"_id": 0, "dedup_key": 0},
        ).sort("filing_date", -1).limit(limit)

        filings = list(cursor)
        return {"ticker": ticker, "count": len(filings), "filings": filings}


# =============================================================
# GET /v1/fused — Fused events from MongoDB
# =============================================================

@app.get("/v1/fused")
def get_fused_events(
    ticker: str = Query(..., description="Ticker symbol"),
    limit: int = Query(20, ge=1, le=500),
):
    """Retrieve temporally fused price+news events."""
    db = get_mongo_db()
    cursor = db.fused_events.find(
        {"ticker": ticker.upper()},
        {"_id": 0},
    ).sort("timestamp_ms", -1).limit(limit)

    events = list(cursor)
    return {"ticker": ticker, "count": len(events), "events": events}


# =============================================================
# GET /v1/stats — Pipeline statistics
# =============================================================

@app.get("/v1/stats")
def get_stats():
    """Return counts from all data stores for monitoring."""
    stats = {}

    # PostgreSQL stats
    try:
        conn = get_postgres_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as count FROM price_data")
            stats["price_rows"] = cur.fetchone()["count"]
            cur.execute("SELECT COUNT(*) as count FROM companies")
            stats["companies_total"] = cur.fetchone()["count"]
            cur.execute("SELECT COUNT(*) as count FROM companies WHERE is_active = TRUE")
            stats["companies_active"] = cur.fetchone()["count"]
            cur.execute("SELECT COUNT(DISTINCT ticker) as count FROM price_data")
            stats["tickers_with_prices"] = cur.fetchone()["count"]
        conn.close()
    except Exception as e:
        stats["postgres_error"] = str(e)

    # MongoDB stats
    try:
        db = get_mongo_db()
        stats["news_articles"] = db.news_articles.count_documents({})
        # [DISABLED 2026-05-28] SEC EDGAR out of scope.
        # stats["sec_filings"] = db.sec_filings.count_documents({})
        stats["transcripts"] = db.transcripts.count_documents({})
        stats["fused_events"] = db.fused_events.count_documents({})
        stats["sentiment_scores"] = db.sentiment_scores.count_documents({})
    except Exception as e:
        stats["mongo_error"] = str(e)

    return stats


# =============================================================
# GET /v1/sentiment — Sentiment scores from MongoDB
# =============================================================

@app.get("/v1/sentiment")
def get_sentiment(
    ticker: str = Query(..., description="Ticker symbol"),
    model: Optional[str] = Query(None, description="Model name filter: finbert or roberta"),
    limit: int = Query(50, ge=1, le=500),
):
    """Retrieve sentiment scores for a ticker."""
    db = get_mongo_db()
    query = {"ticker": ticker.upper()}
    if model:
        # Allow shorthand: "finbert" -> full model name
        if model.lower() == "finbert":
            query["model"] = "ProsusAI/finbert"
        elif model.lower() == "roberta":
            query["model"] = "mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis"
        else:
            query["model"] = model

    cursor = db.sentiment_scores.find(
        query,
        {"_id": 0, "dedup_key": 0},
    ).sort("scored_at", -1).limit(limit)

    scores = list(cursor)
    return {"ticker": ticker, "count": len(scores), "scores": scores}


# =============================================================
# GET /v1/eodhd/usage — Proxy to EODHD user API (avoids CORS)
# =============================================================

@app.get("/v1/eodhd/usage")
def get_eodhd_usage():
    """Proxy EODHD API usage stats (browser can't call EODHD directly due to CORS)."""
    token = os.environ.get("EODHD_API_TOKEN", "")
    if not token:
        raise HTTPException(status_code=500, detail="EODHD_API_TOKEN not configured")
    try:
        resp = http_requests.get(
            f"https://eodhd.com/api/user?api_token={token}&fmt=json",
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"EODHD API error: {e}")
