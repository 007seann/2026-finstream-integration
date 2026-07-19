"""
DAG (historical / disabled): GDELT GKG 2.0 Deep-History Backfill
=================================================================
One-shot backfill of GDELT GKG 2.0 snapshots, going back as far as the
public masterfilelist allows (GKG 2.0 begins 18 February 2015).

Design intent
-------------
* Runs ONCE, on the Azure VM only, after project handoff. Seeds a
  historical open-data event graph corpus for RAs to query.
* Left DISABLED (schedule=None, paused-on-create). GDELT is free (no API
  key, no rate limit) so accidental triggering costs nothing in money, but
  the ~10 years of 15-min snapshots is ~350k zips, ~200 GB downloaded and
  a multi-day runtime. Do NOT run on the local dev environment.

How it works
------------
1. Downloads GDELT's masterfilelist:
     http://data.gdeltproject.org/gdeltv2/masterfilelist.txt
   Each line: <size>  <hash>  <url>   for one of three feeds:
     *.export.CSV.zip    events
     *.mentions.CSV.zip  event mentions
     *.gkg.csv.zip       global knowledge graph  <-- this DAG only fetches these
2. Filters to *.gkg.csv.zip URLs whose timestamp falls in the configured
   [from, to] window.
3. Downloads and parses each zip using the same _parse_gkg_row helper
   already unit-tested in gdelt_news_dag.py.
4. Upserts into MongoDB news_articles with source="gdelt_historical".

Environment variables
---------------------
    GDELT_HISTORICAL_FROM         YYYYMMDDHHMMSS, default 20150218000000
                                  (GKG 2.0 earliest snapshot)
    GDELT_HISTORICAL_TO           YYYYMMDDHHMMSS, default 20260101000000
    GDELT_HISTORICAL_MAX_SNAPSHOTS  safety cap on files processed, default 200
                                    (raise for the full 10-year run on Azure)

Runtime estimate
----------------
* ~35,040 GKG snapshots per year x 10 years = ~350k snapshots.
* Each snapshot ~500 KB compressed, ~5 MB uncompressed. Full corpus ~180 GB.
* At ~5s / snapshot (download + parse + upsert), full backfill = ~20 days
  of continuous processing. Chunk with GDELT_HISTORICAL_MAX_SNAPSHOTS.
"""

from __future__ import annotations

import os
import io
import csv
import zipfile
import logging
import re
from datetime import datetime, timezone

import requests
import pendulum
from airflow.decorators import dag, task

from gdelt_news_dag import _parse_gkg_row, _parse_tone   # reuse tested helpers

logger = logging.getLogger(__name__)

MASTERFILELIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
GKG_URL_PATTERN = re.compile(r"(\d{14})\.gkg\.csv\.zip$")


@dag(
    dag_id="gdelt_historical_pipeline",
    schedule=None,                 # DISABLED — manual one-shot only
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,  # extra safety belt: starts paused
    tags=["ingestion", "news", "gdelt", "historical", "disabled"],
    doc_md="""
    ### GDELT Historical Backfill (DISABLED by default)
    One-shot deep-history GDELT GKG 2.0 backfill via masterfilelist.txt.
    Intended for the Azure VM only, after project handoff.
    Free but ~180 GB / ~20 days runtime for full 10-year window;
    chunk with GDELT_HISTORICAL_MAX_SNAPSHOTS.
    """,
)
def gdelt_historical_pipeline():

    @task
    def list_gkg_snapshots() -> list[str]:
        """
        Read masterfilelist.txt and return the GKG URLs within the configured
        [from, to] window, capped at GDELT_HISTORICAL_MAX_SNAPSHOTS.
        """
        from common.constants import GDELT_HTTP_TIMEOUT_S

        from_ts = os.environ.get("GDELT_HISTORICAL_FROM", "20150218000000")
        to_ts = os.environ.get("GDELT_HISTORICAL_TO", "20260101000000")
        max_snapshots = int(os.environ.get("GDELT_HISTORICAL_MAX_SNAPSHOTS", "200"))

        logger.info(f"Historical GDELT window: {from_ts} -> {to_ts} (max {max_snapshots} snapshots)")

        resp = requests.get(MASTERFILELIST_URL, timeout=GDELT_HTTP_TIMEOUT_S)
        resp.raise_for_status()

        selected: list[str] = []
        for line in resp.text.splitlines():
            parts = line.split()
            if not parts:
                continue
            url = parts[-1]
            match = GKG_URL_PATTERN.search(url)
            if not match:
                continue
            snapshot_ts = match.group(1)
            if from_ts <= snapshot_ts <= to_ts:
                selected.append(url)
            if len(selected) >= max_snapshots:
                logger.info(f"Reached MAX_SNAPSHOTS cap ({max_snapshots})")
                break

        logger.info(f"Selected {len(selected)} GKG snapshots to backfill")
        return selected

    @task
    def load_org_to_ticker_map() -> dict:
        """Reuse the same org->ticker lookup pattern from gdelt_news_dag."""
        from common.constants import ORG_NAME_SUFFIXES
        from common.db_utils import get_postgres_conn

        conn = get_postgres_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT ticker, company_name FROM companies")
                rows = cur.fetchall()
        finally:
            conn.close()

        org_map: dict[str, str] = {}
        for ticker, name in rows:
            if not name:
                continue
            norm = name.lower().strip()
            org_map.setdefault(norm, ticker)
            for suffix in ORG_NAME_SUFFIXES:
                if norm.endswith(suffix):
                    base = norm[: -len(suffix)].strip(" .,")
                    if base and len(base) >= 3:
                        org_map.setdefault(base, ticker)
                    break

        logger.info(f"Org->ticker map: {len({t for t in org_map.values()})} tickers, "
                    f"{len(org_map)} name variants")
        return org_map

    @task
    def download_parse_upsert(gkg_urls: list[str], org_to_ticker: dict) -> dict:
        """
        Sequentially download each GKG snapshot, parse it, and upsert
        matching articles into MongoDB news_articles with
        source="gdelt_historical".
        """
        from common.constants import (
            GDELT_HTTP_TIMEOUT_S,
            GDELT_GKG_FIELD_COUNT,
            GDELT_FINANCIAL_THEMES,
            NEWS_COLLECTION,
        )
        from common.db_utils import upsert_documents

        if not gkg_urls:
            logger.warning("No GKG URLs selected; nothing to backfill.")
            return {"snapshots": 0, "articles": 0, "inserted": 0}

        total_articles = 0
        total_inserted = 0
        snapshots_processed = 0

        for gkg_url in gkg_urls:
            snapshots_processed += 1
            try:
                resp = requests.get(gkg_url, timeout=GDELT_HTTP_TIMEOUT_S)
                resp.raise_for_status()

                snapshot_articles: list[dict] = []
                with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                    csv_name = next((n for n in zf.namelist() if n.endswith(".csv")), None)
                    if not csv_name:
                        continue
                    with zf.open(csv_name) as raw:
                        text = io.TextIOWrapper(raw, encoding="utf-8", errors="ignore")
                        reader = csv.reader(text, delimiter="\t")
                        for row in reader:
                            if len(row) < GDELT_GKG_FIELD_COUNT:
                                continue
                            try:
                                parsed = _parse_gkg_row(row, org_to_ticker, GDELT_FINANCIAL_THEMES)
                            except Exception:
                                continue
                            if parsed and parsed.get("per_ticker_docs"):
                                for doc in parsed["per_ticker_docs"]:
                                    doc["source"] = "gdelt_historical"   # override for provenance
                                snapshot_articles.extend(parsed["per_ticker_docs"])

                if snapshot_articles:
                    inserted = upsert_documents(
                        NEWS_COLLECTION, snapshot_articles, dedup_field="dedup_key"
                    )
                    total_articles += len(snapshot_articles)
                    total_inserted += inserted

                if snapshots_processed % 20 == 0:
                    logger.info(
                        f"Progress: {snapshots_processed}/{len(gkg_urls)} snapshots, "
                        f"{total_articles} articles, {total_inserted} inserted"
                    )

            except zipfile.BadZipFile:
                logger.warning(f"Corrupt zip skipped: {gkg_url}")
                continue
            except Exception as e:
                logger.error(f"Snapshot fetch failed ({gkg_url}): {e}")
                continue

        summary = {
            "snapshots": snapshots_processed,
            "articles":  total_articles,
            "inserted":  total_inserted,
        }
        logger.info(f"Historical GDELT backfill complete: {summary}")
        return summary

    gkg_urls = list_gkg_snapshots()
    org_map = load_org_to_ticker_map()
    download_parse_upsert(gkg_urls, org_map)


gdelt_historical_pipeline()
