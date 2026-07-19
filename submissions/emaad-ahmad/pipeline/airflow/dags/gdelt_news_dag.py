"""
DAG 4: GDELT GKG 2.0 News Ingestion Pipeline
==============================================
Fetches the latest 15-minute Global Knowledge Graph snapshot from GDELT
(https://www.gdeltproject.org/), filters articles by financial themes and
S&P 500 company-name matches, and stores them in MongoDB `news_articles`
with `source: "gdelt"`.

Cost:    Free. No API key required.
Cadence: GDELT publishes a fresh GKG snapshot every 15 minutes (at :00, :15,
         :30, :45 UTC). This DAG mirrors that schedule.
Volume:  Each GKG snapshot contains ~150-300k rows globally; after theme +
         ticker filtering, expect ~20-200 documents per run.

Data flow:
  GDELT lastupdate.txt
        ↓
  Download newest *.gkg.csv.zip
        ↓
  Parse 27-column TSV (V1Themes, V1Organizations, V1.5Tone, V2.1DATE, URL, ...)
        ↓
  Filter: at least one financial theme AND at least one S&P 500 company name
        ↓
  Normalise into platform schema  (one doc per (article, matched_ticker) pair)
        ↓
  Upsert into MongoDB `news_articles`  (dedup_key = "gdelt|{gkg_id}|{ticker}")

GKG fields used (0-indexed) — see GDELT GKG 2.1 codebook:
  0   GKGRECORDID              unique snapshot+sequence ID
  1   V2.1DATE                 YYYYMMDDHHMMSS (article publication time)
  3   V2SourceCommonName       publisher name (e.g. "reuters.com")
  4   V2DocumentIdentifier     article URL
  7   V1Themes                 ;-delimited theme tags
  13  V1Organizations          ;-delimited org names
  15  V1.5Tone                 csv: tone,positive,negative,polarity,activity,selfref,wordcount
"""

from __future__ import annotations

import os
import io
import csv
import zipfile
import logging
from datetime import datetime, timezone

import requests
import pendulum
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# GKG column positions (0-indexed) — see GDELT GKG 2.1 codebook
# ---------------------------------------------------------------------------
COL_GKG_RECORD_ID    = 0
COL_DATE             = 1
COL_SOURCE_NAME      = 3
COL_DOCUMENT_URL     = 4
COL_V1_THEMES        = 7
COL_V1_ORGANIZATIONS = 13
COL_V15_TONE         = 15


@dag(
    dag_id="gdelt_news_pipeline",
    schedule="*/15 * * * *",     # GDELT publishes every 15 min
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,           # Prevent overlapping runs
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=2),
    },
    tags=["ingestion", "news", "gdelt", "free"],
    doc_md="""
    ### GDELT GKG 2.0 News Pipeline
    Pulls the latest 15-minute GDELT Global Knowledge Graph snapshot, keeps
    articles tagged with financial themes that mention an S&P 500 company,
    and stores them as `source: "gdelt"` documents alongside EODHD news.

    **Free** — no API key, no rate limit. Acts as a heterogeneous,
    open-data complement to the commercial EODHD news feed.
    """,
)
def gdelt_news_pipeline():

    # -----------------------------------------------------------------------
    # Task 1: discover the URL of the newest GKG snapshot
    # -----------------------------------------------------------------------
    @task
    def get_latest_gkg_url() -> str:
        """
        Read GDELT's `lastupdate.txt` and return the URL of the newest GKG zip.

        lastupdate.txt has 3 lines (export, mentions, gkg). Format per line:
            <size>  <hash>  <url>
        """
        from common.constants import GDELT_LASTUPDATE_URL, GDELT_HTTP_TIMEOUT_S

        resp = requests.get(GDELT_LASTUPDATE_URL, timeout=GDELT_HTTP_TIMEOUT_S)
        resp.raise_for_status()

        for line in resp.text.strip().splitlines():
            parts = line.split()
            if not parts:
                continue
            url = parts[-1]
            if url.endswith("gkg.csv.zip"):
                logger.info("Latest GKG snapshot: %s", url)
                return url

        raise ValueError("No gkg.csv.zip URL found in GDELT lastupdate.txt")

    # -----------------------------------------------------------------------
    # Task 2: build org_name → ticker lookup from PostgreSQL companies table
    # -----------------------------------------------------------------------
    @task
    def load_org_to_ticker_map() -> dict:
        """
        Build a normalised company-name → ticker lookup from the `companies`
        table in PostgreSQL. Adds suffix-stripped variants so that GDELT's
        "Apple Inc" matches our "Apple Inc.", and "Microsoft" matches
        "Microsoft Corporation".
        """
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
            # Add suffix-stripped variant (e.g. "apple inc" → "apple")
            for suffix in ORG_NAME_SUFFIXES:
                if norm.endswith(suffix):
                    base = norm[: -len(suffix)].strip(" .,")
                    if base and len(base) >= 3:
                        org_map.setdefault(base, ticker)
                    break

        logger.info(
            "Org→ticker map: %d tickers, %d name variants",
            len({t for t in org_map.values()}), len(org_map),
        )
        return org_map

    # -----------------------------------------------------------------------
    # Task 3: download, parse, filter, normalise
    # -----------------------------------------------------------------------
    @task
    def fetch_and_parse_gkg(gkg_url: str, org_to_ticker: dict) -> list[dict]:
        """
        Download the GKG zip, parse the 27-column TSV, and emit one
        platform-shaped document per (article × matched ticker) pair.
        """
        from common.constants import (
            GDELT_HTTP_TIMEOUT_S,
            GDELT_GKG_FIELD_COUNT,
            GDELT_FINANCIAL_THEMES,
        )

        if not org_to_ticker:
            logger.warning("Empty org→ticker map; nothing to match against. Aborting fetch.")
            return []

        resp = requests.get(gkg_url, timeout=GDELT_HTTP_TIMEOUT_S)
        resp.raise_for_status()

        articles: list[dict] = []
        rows_seen = 0
        rows_theme_match = 0
        rows_org_match = 0

        try:
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                csv_name = next((n for n in zf.namelist() if n.endswith(".csv")), None)
                if not csv_name:
                    raise ValueError(f"No .csv file inside zip: {gkg_url}")

                with zf.open(csv_name) as raw:
                    text = io.TextIOWrapper(raw, encoding="utf-8", errors="ignore")
                    reader = csv.reader(text, delimiter="\t")

                    for row in reader:
                        rows_seen += 1
                        if len(row) < GDELT_GKG_FIELD_COUNT:
                            continue

                        try:
                            doc = _parse_gkg_row(row, org_to_ticker, GDELT_FINANCIAL_THEMES)
                        except Exception as e:
                            logger.debug("Row %d skipped: %s", rows_seen, e)
                            continue

                        if doc is None:
                            continue

                        # `_parse_gkg_row` may match multiple tickers — flatten.
                        rows_theme_match += 1
                        if doc["matched_tickers"]:
                            rows_org_match += 1
                            articles.extend(doc["per_ticker_docs"])

        except zipfile.BadZipFile:
            logger.error("Corrupt GKG zip: %s", gkg_url)
            raise

        logger.info(
            "GDELT scan complete: %d rows seen | %d passed theme filter "
            "| %d passed org filter | %d documents emitted",
            rows_seen, rows_theme_match, rows_org_match, len(articles),
        )
        return articles

    # -----------------------------------------------------------------------
    # Task 4: upsert into MongoDB news_articles
    # -----------------------------------------------------------------------
    @task
    def store_articles(articles: list[dict]) -> dict:
        """Upsert articles into MongoDB news_articles, dedup'd by gkg_id+ticker."""
        if not articles:
            return {"total": 0, "inserted": 0}

        from common.constants import NEWS_COLLECTION
        from common.db_utils import upsert_documents

        inserted = upsert_documents(NEWS_COLLECTION, articles, dedup_field="dedup_key")
        logger.info("GDELT store: %d inserted / %d total", inserted, len(articles))
        return {"total": len(articles), "inserted": inserted}

    # -----------------------------------------------------------------------
    # DAG flow
    # -----------------------------------------------------------------------
    url = get_latest_gkg_url()
    org_map = load_org_to_ticker_map()
    arts = fetch_and_parse_gkg(url, org_map)
    store_articles(arts)


# ---------------------------------------------------------------------------
# Row-level parser (kept at module level for unit testability)
# ---------------------------------------------------------------------------
def _parse_gkg_row(row: list[str], org_to_ticker: dict, financial_themes: set) -> dict | None:
    """
    Parse one GKG TSV row. Returns None if the row fails any filter.

    On success returns:
        {
            "matched_themes":  set of theme tags matched,
            "matched_tickers": list of tickers matched,
            "per_ticker_docs": list of normalised documents (one per ticker),
        }
    """
    gkg_id     = row[COL_GKG_RECORD_ID]
    date_str   = row[COL_DATE]
    sourcename = row[COL_SOURCE_NAME]
    url        = row[COL_DOCUMENT_URL]
    v1themes   = row[COL_V1_THEMES]
    v1orgs     = row[COL_V1_ORGANIZATIONS]
    v15tone    = row[COL_V15_TONE]

    if not url or not date_str or not gkg_id:
        return None

    # Filter 1: must mention at least one financial theme
    themes_in_row = {t for t in v1themes.split(";") if t}
    matched_themes = themes_in_row & financial_themes
    if not matched_themes:
        return None

    # Filter 2: at least one S&P 500 org match
    matched_tickers: set[str] = set()
    for raw_org in v1orgs.split(";"):
        org = raw_org.strip().lower()
        if not org:
            continue
        if org in org_to_ticker:
            matched_tickers.add(org_to_ticker[org])
    if not matched_tickers:
        return {"matched_themes": matched_themes, "matched_tickers": [], "per_ticker_docs": []}

    # Parse publication timestamp (YYYYMMDDHHMMSS)
    try:
        dt = datetime.strptime(date_str, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None

    # Parse V1.5 tone (csv: tone, positive, negative, polarity, activity, selfref, wordcount)
    tone = _parse_tone(v15tone)

    ingested_at = datetime.now(timezone.utc).isoformat()
    tickers_sorted = sorted(matched_tickers)
    themes_sorted  = sorted(matched_themes)

    per_ticker_docs = [
        {
            "ticker":            ticker,
            "title":             "",          # GKG provides metadata, not headline
            "content":           "",          # GKG provides metadata, not body
            "published_at":      dt.isoformat(),
            "url":               url,
            "publisher":         sourcename,
            "symbols_mentioned": tickers_sorted,
            "tags":              themes_sorted,
            "gdelt_tone":        tone,
            "gdelt_record_id":   gkg_id,
            "eodhd_sentiment":   None,        # GDELT articles have no EODHD score
            "dedup_key":         f"gdelt|{gkg_id}|{ticker}",
            "ingested_at":       ingested_at,
            "source":            "gdelt",
        }
        for ticker in tickers_sorted
    ]

    return {
        "matched_themes":  matched_themes,
        "matched_tickers": tickers_sorted,
        "per_ticker_docs": per_ticker_docs,
    }


def _parse_tone(v15tone: str) -> dict:
    """
    Parse GDELT V1.5 tone column.

    Format: "tone,positive,negative,polarity,activity_ref_density,self_group_ref_density,word_count"
      tone        = positive - negative      (range roughly -100 to +100, typical -10..+10)
      polarity    = positive + negative      (emotional intensity)
      word_count  = count of words in article
    """
    parts = v15tone.split(",")
    tone: dict = {}
    try:
        if len(parts) >= 1 and parts[0]:
            tone["tone_score"] = float(parts[0])
        if len(parts) >= 2 and parts[1]:
            tone["positive"] = float(parts[1])
        if len(parts) >= 3 and parts[2]:
            tone["negative"] = float(parts[2])
        if len(parts) >= 4 and parts[3]:
            tone["polarity"] = float(parts[3])
        if len(parts) >= 5 and parts[4]:
            tone["activity_ref_density"] = float(parts[4])
        if len(parts) >= 7 and parts[6]:
            tone["word_count"] = int(float(parts[6]))
    except (ValueError, IndexError):
        pass
    return tone


gdelt_news_pipeline()
