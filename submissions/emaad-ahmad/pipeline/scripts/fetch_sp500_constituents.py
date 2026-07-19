"""
Fetch the current S&P 500 constituent list and seed it into PostgreSQL.

Source: Wikipedia "List of S&P 500 companies" — stable, updated, free, no key.
Output:
  data/sp500_companies.json   bundled file (committed to repo) — used as offline
                              fallback by tests and as the canonical seed source.
  PostgreSQL `companies` table  populated/refreshed with all ~503 constituents.

Run once (or whenever the index changes):
    python scripts/fetch_sp500_constituents.py             # fetch + write JSON only
    python scripts/fetch_sp500_constituents.py --seed-db   # fetch + write JSON + load into Postgres

Idempotent. Re-running updates the JSON and upserts the DB rows.
"""

from __future__ import annotations
import argparse
import csv
import io
import json
import re
import sys
from pathlib import Path

import requests

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
OUT_PATH = Path(__file__).resolve().parents[1] / "data" / "sp500_companies.json"


def fetch_wikipedia_constituents() -> list[dict]:
    """
    Parse the constituents table from Wikipedia.

    Returns a list of {ticker, company_name, sector, cik} dicts.
    Uses pandas if available for robust HTML-table parsing, falls back to
    regex on the raw HTML if not.
    """
    print(f"[1/3] Fetching {WIKI_URL} ...")
    headers = {"User-Agent": "MSc-IPP-Dissertation-Research/1.0 (contact: ahmad.emaad19@gmail.com)"}
    resp = requests.get(WIKI_URL, headers=headers, timeout=60)
    resp.raise_for_status()

    try:
        import pandas as pd
        print("      using pandas.read_html for parsing")
        tables = pd.read_html(io.StringIO(resp.text))
        df = tables[0]   # first table on the page is the constituents list
        df = df.rename(columns={
            "Symbol":   "ticker",
            "Security": "company_name",
            "GICS Sector": "sector",
            "CIK":      "cik",
        })
        rows = [
            {
                "ticker":       str(r["ticker"]).strip().replace(".", "-"),
                "company_name": str(r["company_name"]).strip(),
                "sector":       str(r.get("sector", "")).strip(),
                "cik":          str(r.get("cik", "")).strip().zfill(10) if r.get("cik") else None,
            }
            for _, r in df.iterrows()
            if str(r.get("ticker")).strip()
        ]
    except ImportError:
        print("      pandas unavailable — falling back to regex parsing")
        rows = _regex_parse_wikipedia(resp.text)

    print(f"      parsed {len(rows)} constituents")
    return rows


def _regex_parse_wikipedia(html: str) -> list[dict]:
    """Minimal regex parser if pandas isn't installed."""
    rows = []
    table_match = re.search(r'<table[^>]*id="constituents".*?</table>', html, re.DOTALL)
    if not table_match:
        raise RuntimeError("Could not locate constituents table in Wikipedia HTML")
    table_html = table_match.group(0)

    row_re = re.compile(r"<tr>(.*?)</tr>", re.DOTALL)
    cell_re = re.compile(r"<t[hd][^>]*>(.*?)</t[hd]>", re.DOTALL)
    tag_re = re.compile(r"<[^>]+>")

    for tr in row_re.findall(table_html)[1:]:   # skip header
        cells = [tag_re.sub("", c).strip() for c in cell_re.findall(tr)]
        if len(cells) >= 2 and cells[0]:
            rows.append({
                "ticker":       cells[0].replace(".", "-"),
                "company_name": cells[1],
                "sector":       cells[2] if len(cells) > 2 else "",
                "cik":          (cells[6].zfill(10) if len(cells) > 6 and cells[6].isdigit() else None),
            })
    return rows


def write_json(rows: list[dict]) -> None:
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f"[2/3] Writing {OUT_PATH} ...")
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)
    print(f"      wrote {len(rows)} rows ({OUT_PATH.stat().st_size / 1024:.1f} KB)")


def seed_postgres(rows: list[dict]) -> None:
    """
    Reconcile the `companies` table against the fetched S&P 500 list.

    Three-way reconciliation:
      1. Tickers in fetched list but NOT in DB        → INSERT (is_active=TRUE, added_at=NOW())
      2. Tickers in DB and in fetched list            → UPDATE name/sector/cik; ensure is_active=TRUE
      3. Tickers in DB but NOT in fetched list        → mark is_active=FALSE, removed_at=NOW()
                                                       (NEVER DELETE — preserves historical data)

    Idempotent: re-running with the same list is a no-op.
    """
    print("[3/3] Seeding PostgreSQL `companies` table ...")
    try:
        import psycopg2
    except ImportError:
        print("      psycopg2 not installed.")
        print("      Quick fix: py -m pip install psycopg2-binary")
        print("      JSON file is still written for offline use.")
        return

    import os
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        user=os.environ.get("POSTGRES_USER", "finplatform"),
        password=os.environ.get("POSTGRES_PASSWORD", "finplatform_dev_2026"),
        dbname=os.environ.get("POSTGRES_DB", "financial_data"),
    )
    try:
        fetched_tickers = {r["ticker"] for r in rows}

        with conn.cursor() as cur:
            # Current DB state
            cur.execute("SELECT ticker, is_active FROM companies")
            db_state = {row[0]: row[1] for row in cur.fetchall()}
            db_tickers = set(db_state.keys())

            # 1 & 2: upsert all fetched tickers (active)
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

            # 3: deactivate tickers in DB but not in current S&P 500 list
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

            # Re-activations (was inactive, now back in list)
            reactivated = [t for t in fetched_tickers
                           if t in db_state and db_state[t] is False]

            inserted = fetched_tickers - db_tickers

        conn.commit()
        print(f"      added            : {len(inserted):>3}  (new constituents)")
        print(f"      updated/active   : {len(fetched_tickers) - len(inserted):>3}")
        print(f"      re-activated     : {len(reactivated):>3}  (re-added to index)")
        print(f"      deactivated      : {len(removed):>3}  (removed from index — data preserved)")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Fetch S&P 500 list and optionally seed PostgreSQL.")
    parser.add_argument("--seed-db", action="store_true", help="Also load rows into PostgreSQL companies table.")
    args = parser.parse_args()

    rows = fetch_wikipedia_constituents()
    if not rows:
        print("ERROR: no rows fetched.", file=sys.stderr)
        return 1
    write_json(rows)
    if args.seed_db:
        seed_postgres(rows)
    else:
        print("[3/3] Skipping DB seed (no --seed-db flag). Run with --seed-db to load into PostgreSQL.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
