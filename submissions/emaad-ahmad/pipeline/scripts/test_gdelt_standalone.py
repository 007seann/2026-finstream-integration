"""
Standalone GDELT GKG fetch+parse test
======================================
Runs the same pipeline as `airflow/dags/gdelt_news_dag.py` but without
Airflow — useful for iterating on filters and inspecting raw output.

Usage:
    python scripts/test_gdelt_standalone.py

Outputs:
    scripts/sample_gdelt_raw_first_5_rows.txt   first 5 raw GKG rows (TSV)
    scripts/sample_gdelt_matches.json           normalised matched docs (first 20)

No API key required. No DB write. Prints a summary at the end.
"""

from __future__ import annotations

import io
import csv
import json
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests

# --- Config (mirrors constants.py) ------------------------------------------
GDELT_LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
HTTP_TIMEOUT = 60
FIELD_COUNT = 27

COL_GKG_RECORD_ID    = 0
COL_DATE             = 1
COL_SOURCE_NAME      = 3
COL_DOCUMENT_URL     = 4
COL_V1_THEMES        = 7
COL_V1_ORGANIZATIONS = 13
COL_V15_TONE         = 15

FINANCIAL_THEMES = {
    "ECON_STOCKMARKET", "ECON_EARNINGSREPORT", "ECON_BANKRUPTCY",
    "ECON_INTEREST_RATES", "ECON_MONETARY_POLICY", "ECON_INFLATION",
    "ECON_TRADE",
    "BUS_STOCK_BUYBACK", "BUS_MARKET_CLOSE", "BUS_NEW_PRODUCTS",
    "MERGER", "ACQUISITION", "LEG_FINANCIAL_REGULATION",
}

SP500_JSON_PATH = Path(__file__).resolve().parents[1] / "data" / "sp500_companies.json"

# Suffixes to strip when normalising company names for fuzzy matching against
# GDELT's V1Organizations field (kept in sync with constants.ORG_NAME_SUFFIXES).
SUFFIXES = (
    " inc.", " inc", " corp.", " corp", " corporation",
    " company", " co.", " co", " ltd.", " ltd", " plc",
    " limited", " group", " holdings", " holding",
)


def load_org_to_ticker_map() -> dict:
    """
    Load full S&P 500 from the bundled JSON file. Falls back to a small demo
    set if the JSON isn't present (so the script still runs out-of-the-box).
    """
    if SP500_JSON_PATH.exists():
        with open(SP500_JSON_PATH, encoding="utf-8") as f:
            rows = json.load(f)
        org_map: dict[str, str] = {}
        for r in rows:
            ticker = (r.get("ticker") or "").strip().upper()
            name   = (r.get("company_name") or "").strip()
            if not ticker or not name:
                continue
            norm = name.lower()
            org_map.setdefault(norm, ticker)
            for suffix in SUFFIXES:
                if norm.endswith(suffix):
                    base = norm[: -len(suffix)].strip(" .,")
                    if base and len(base) >= 3:
                        org_map.setdefault(base, ticker)
                    break
        print(f"  Loaded {len({t for t in org_map.values()})} tickers "
              f"with {len(org_map)} name variants from sp500_companies.json")
        return org_map

    print("  sp500_companies.json not found — using 10-ticker demo fallback.")
    print("  Run `python scripts/fetch_sp500_constituents.py` to generate the full list.")
    return {
        "apple":              "AAPL", "apple inc":     "AAPL", "apple inc.":      "AAPL",
        "microsoft":          "MSFT", "microsoft corp": "MSFT", "microsoft corporation": "MSFT",
        "google":             "GOOGL", "alphabet":     "GOOGL", "alphabet inc":    "GOOGL",
        "amazon":             "AMZN", "amazon.com":   "AMZN",
        "nvidia":             "NVDA", "nvidia corp":  "NVDA", "nvidia corporation": "NVDA",
        "tesla":              "TSLA", "tesla inc":    "TSLA",
        "meta":               "META", "meta platforms": "META", "facebook":       "META",
        "jpmorgan":           "JPM",  "jpmorgan chase": "JPM",  "jp morgan":      "JPM",
        "visa":               "V",
        "johnson & johnson":  "JNJ",
    }


def get_latest_gkg_url() -> str:
    resp = requests.get(GDELT_LASTUPDATE_URL, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    for line in resp.text.strip().splitlines():
        parts = line.split()
        if parts and parts[-1].endswith("gkg.csv.zip"):
            return parts[-1]
    raise ValueError("No gkg.csv.zip URL in lastupdate.txt")


def parse_tone(v15tone: str) -> dict:
    parts = v15tone.split(",")
    tone = {}
    try:
        if len(parts) >= 1 and parts[0]:   tone["tone_score"] = float(parts[0])
        if len(parts) >= 2 and parts[1]:   tone["positive"]   = float(parts[1])
        if len(parts) >= 3 and parts[2]:   tone["negative"]   = float(parts[2])
        if len(parts) >= 4 and parts[3]:   tone["polarity"]   = float(parts[3])
        if len(parts) >= 7 and parts[6]:   tone["word_count"] = int(float(parts[6]))
    except (ValueError, IndexError):
        pass
    return tone


def main():
    out_dir = Path(__file__).parent
    out_dir.mkdir(exist_ok=True)

    print("[1/5] Loading S&P 500 org→ticker map...")
    org_to_ticker = load_org_to_ticker_map()

    print("[2/5] Fetching latest GKG snapshot URL...")
    url = get_latest_gkg_url()
    print(f"      → {url}")

    print("[3/5] Downloading GKG zip...")
    resp = requests.get(url, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    size_mb = len(resp.content) / 1024 / 1024
    print(f"      → {size_mb:.1f} MB")

    print("[4/5] Parsing TSV, filtering, normalising...")
    matches = []
    rows_seen = 0
    rows_theme_match = 0
    rows_org_match = 0
    first_5_raw = []

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        csv_name = next(n for n in zf.namelist() if n.endswith(".csv"))
        with zf.open(csv_name) as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8", errors="ignore")
            reader = csv.reader(text, delimiter="\t")
            for row in reader:
                rows_seen += 1
                if len(first_5_raw) < 5:
                    first_5_raw.append(row)

                if len(row) < FIELD_COUNT:
                    continue

                v1themes = row[COL_V1_THEMES]
                themes_in_row = {t for t in v1themes.split(";") if t}
                matched_themes = themes_in_row & FINANCIAL_THEMES
                if not matched_themes:
                    continue
                rows_theme_match += 1

                v1orgs = row[COL_V1_ORGANIZATIONS]
                matched_tickers = set()
                for raw_org in v1orgs.split(";"):
                    org = raw_org.strip().lower()
                    if org in org_to_ticker:
                        matched_tickers.add(org_to_ticker[org])
                if not matched_tickers:
                    continue
                rows_org_match += 1

                try:
                    dt = datetime.strptime(row[COL_DATE], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
                except ValueError:
                    continue

                tone = parse_tone(row[COL_V15_TONE])
                for ticker in sorted(matched_tickers):
                    matches.append({
                        "ticker":            ticker,
                        "title":             "",
                        "content":           "",
                        "published_at":      dt.isoformat(),
                        "url":               row[COL_DOCUMENT_URL],
                        "publisher":         row[COL_SOURCE_NAME],
                        "symbols_mentioned": sorted(matched_tickers),
                        "tags":              sorted(matched_themes),
                        "gdelt_tone":        tone,
                        "gdelt_record_id":   row[COL_GKG_RECORD_ID],
                        "dedup_key":         f"gdelt|{row[COL_GKG_RECORD_ID]}|{ticker}",
                        "source":            "gdelt",
                    })

    print(f"      rows seen        = {rows_seen}")
    print(f"      theme-filtered   = {rows_theme_match}")
    print(f"      org-filtered     = {rows_org_match}")
    print(f"      documents emitted = {len(matches)}  (one per (article, ticker) pair)")

    print("[5/5] Writing samples to disk...")
    raw_path = out_dir / "sample_gdelt_raw_first_5_rows.txt"
    with open(raw_path, "w", encoding="utf-8") as f:
        for r in first_5_raw:
            f.write("\t".join(r) + "\n\n")
    print(f"      → {raw_path}")

    matches_path = out_dir / "sample_gdelt_matches.json"
    with open(matches_path, "w", encoding="utf-8") as f:
        json.dump(matches[:20], f, indent=2, ensure_ascii=False)
    print(f"      → {matches_path}  (first 20 of {len(matches)})")

    if matches:
        print("\nSample document:")
        print(json.dumps(matches[0], indent=2, ensure_ascii=False))
    else:
        print("\nNo matches in this snapshot. Try running again in 15 min — financial")
        print("news volume varies. Or expand FINANCIAL_THEMES / DEMO_ORG_TO_TICKER.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
