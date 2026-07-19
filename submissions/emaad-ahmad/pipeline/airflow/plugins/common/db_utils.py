"""
Database utility functions for the financial data platform.
Provides connections to PostgreSQL (prices) and MongoDB (text data).
Database utilities for the ingestion layer.
"""

from __future__ import annotations

import os
import logging
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

logger = logging.getLogger(__name__)


# =============================================================
# PostgreSQL (price data)
# =============================================================

def get_postgres_conn():
    """Get a psycopg2 connection to the financial_data database."""
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        user=os.environ.get("POSTGRES_USER", "finplatform"),
        password=os.environ.get("POSTGRES_PASSWORD", "finplatform_dev_2026"),
        dbname=os.environ.get("POSTGRES_DB", "financial_data"),
    )


def get_active_sp500_tickers() -> list[str]:
    """
    Return the list of CURRENTLY ACTIVE S&P 500 tickers.

    Every ingestion DAG should call this rather than hardcoding lists. Filters
    out tickers that have been removed from the index (is_active=FALSE), so
    we stop polling them — but their historical data remains queryable.
    """
    conn = get_postgres_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ticker FROM companies WHERE is_active = TRUE ORDER BY ticker"
            )
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def get_active_sp500_with_names() -> list[dict]:
    """Like `get_active_sp500_tickers` but also returns names + sectors + CIKs."""
    conn = get_postgres_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ticker, company_name, sector, cik
                  FROM companies
                 WHERE is_active = TRUE
                 ORDER BY ticker
                """
            )
            return [
                {"ticker": r[0], "company_name": r[1], "sector": r[2], "cik": r[3]}
                for r in cur.fetchall()
            ]
    finally:
        conn.close()


def insert_prices(rows: list[dict]):
    """
    Batch-insert OHLCV price rows into PostgreSQL.
    Uses ON CONFLICT to skip duplicates (same ticker + timestamp + interval).

    Each row: {ticker, timestamp_ms, datetime_utc, open, high, low, close, volume, interval, source}
    """
    if not rows:
        return 0

    conn = get_postgres_conn()
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO price_data
                    (ticker, timestamp_ms, datetime_utc, open, high, low, close, volume, interval, source)
                VALUES %s
                ON CONFLICT (ticker, timestamp_ms, interval) DO NOTHING
            """
            values = [
                (
                    r["ticker"],
                    r["timestamp_ms"],
                    r["datetime_utc"],
                    r.get("open"),
                    r.get("high"),
                    r.get("low"),
                    r.get("close"),
                    r.get("volume"),
                    r.get("interval", "1d"),
                    r.get("source", "eodhd"),
                )
                for r in rows
            ]
            execute_values(cur, sql, values)
            inserted = cur.rowcount
        conn.commit()
        logger.info(f"Inserted {inserted}/{len(rows)} price rows")
        return inserted
    finally:
        conn.close()


def log_pipeline_run(dag_id: str, records: int, latency_s: float, status: str, error: str = None):
    """Log a pipeline execution for SLA tracking (RQ1)."""
    conn = get_postgres_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs
                    (dag_id, start_time, end_time, records_ingested, latency_seconds, status, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (dag_id, datetime.now(timezone.utc), datetime.now(timezone.utc),
                 records, latency_s, status, error),
            )
        conn.commit()
    finally:
        conn.close()


# =============================================================
# MongoDB (text data: news, SEC, fused events)
# =============================================================

def get_mongo_client():
    """Get a MongoClient connected to the financial_db database."""
    host = os.environ.get("MONGO_HOST", "mongodb")
    port = int(os.environ.get("MONGO_PORT", 27017))
    user = os.environ.get("MONGO_INITDB_ROOT_USERNAME", "finplatform")
    pwd = os.environ.get("MONGO_INITDB_ROOT_PASSWORD", "finplatform_dev_2026")
    db_name = os.environ.get("MONGO_DB", "financial_db")

    client = MongoClient(
        f"mongodb://{user}:{pwd}@{host}:{port}/",
        serverSelectionTimeoutMS=5000,
    )
    return client, client[db_name]


def upsert_documents(collection_name: str, docs: list[dict], dedup_field: str = "dedup_key"):
    """
    Insert documents into MongoDB, skipping duplicates based on dedup_field.
    Pattern adapted from RA platform's push_news.py.
    """
    if not docs:
        return 0

    client, db = get_mongo_client()
    try:
        collection = db[collection_name]

        # Get existing dedup keys in one batch query
        dedup_values = [d[dedup_field] for d in docs if dedup_field in d]
        existing = set()
        if dedup_values:
            cursor = collection.find(
                {dedup_field: {"$in": dedup_values}},
                {dedup_field: 1}
            )
            existing = {doc[dedup_field] for doc in cursor}

        # Filter out duplicates
        new_docs = [d for d in docs if d.get(dedup_field) not in existing]

        if new_docs:
            try:
                result = collection.insert_many(new_docs, ordered=False)
                inserted = len(result.inserted_ids)
            except BulkWriteError as bwe:
                # Some docs hit duplicate key (race condition between concurrent runs)
                inserted = bwe.details.get("nInserted", 0)
                logger.warning(f"BulkWriteError: {inserted} inserted, "
                               f"{len(bwe.details.get('writeErrors', []))} duplicates skipped")
            logger.info(f"Inserted {inserted}/{len(docs)} docs into {collection_name}")
            return inserted
        else:
            logger.info(f"All {len(docs)} docs already exist in {collection_name}")
            return 0
    finally:
        client.close()
