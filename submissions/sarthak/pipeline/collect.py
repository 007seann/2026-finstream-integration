"""
Form 4 collection stage.

Downloads one SEC EDGAR quarterly insider-transactions ZIP (bulk Form 3/4/5
flat files) and filters its SUBMISSION / REPORTINGOWNER / NONDERIV_TRANS
tables down to a configured universe of issuer CIKs. No authentication is
required for this endpoint; SEC's fair-access policy only requires a
descriptive User-Agent header (see config.example.yaml).
"""
import io
import logging
import time
import zipfile
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)


class QuarterNotAvailable(Exception):
    """Raised when SEC hasn't published this quarter's file yet (HTTP 404)."""


def load_universe(universe_csv: str):
    """Returns (cik_set, cik_to_ticker) from a CSV with a CIK column and a
    ticker/symbol column (column names are matched case-insensitively so this
    tolerates minor header variations)."""
    universe = pd.read_csv(universe_csv)
    cik_col = next(c for c in universe.columns if "cik" in c.lower())
    ticker_col = next(c for c in universe.columns if any(k in c.lower() for k in ("ticker", "symbol", "tic")))

    universe[cik_col] = pd.to_numeric(universe[cik_col], errors="coerce")
    universe = universe.dropna(subset=[cik_col])
    universe[cik_col] = universe[cik_col].astype(int)

    cik_set = set(universe[cik_col])
    cik_to_ticker = dict(zip(universe[cik_col], universe[ticker_col]))
    return cik_set, cik_to_ticker


def fetch_quarter_zip(sec_cfg: dict, year: int, quarter: int) -> zipfile.ZipFile:
    """Downloads one quarterly ZIP, retrying transient failures. Raises
    QuarterNotAvailable if SEC hasn't published it yet (HTTP 404) — that is
    not retried, since it won't resolve within this run."""
    url = sec_cfg["url_template"].format(year=year, quarter=quarter)
    headers = {"User-Agent": sec_cfg["user_agent"]}
    max_retries = sec_cfg.get("max_retries", 3)
    sleep_sec = sec_cfg.get("request_sleep_sec", 0.5)

    last_error: Optional[BaseException] = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=sec_cfg["request_timeout_sec"])
        except requests.RequestException as exc:
            last_error = exc
            logger.warning("Attempt %d/%d for %s failed: %s", attempt, max_retries, url, exc)
            time.sleep(sleep_sec * attempt)
            continue

        if resp.status_code == 404:
            raise QuarterNotAvailable(f"{year}Q{quarter} not yet published")
        if resp.status_code != 200:
            last_error = RuntimeError(f"HTTP {resp.status_code}")
            logger.warning("Attempt %d/%d for %s: HTTP %d", attempt, max_retries, url, resp.status_code)
            time.sleep(sleep_sec * attempt)
            continue

        return zipfile.ZipFile(io.BytesIO(resp.content))

    raise RuntimeError(f"Failed to fetch {url} after {max_retries} attempts") from last_error


def _read_table(zf: zipfile.ZipFile, name_fragment: str) -> Optional[pd.DataFrame]:
    name = next((n for n in zf.namelist() if name_fragment in n.lower()), None)
    if name is None:
        return None
    df = pd.read_csv(io.BytesIO(zf.read(name)), sep="\t", low_memory=False, on_bad_lines="skip")
    df.columns = df.columns.str.lower().str.strip()
    return df


def collect_quarter(zf: zipfile.ZipFile, cik_set: set) -> Optional[dict]:
    """Filters this quarter's SUBMISSION/REPORTINGOWNER/NONDERIV_TRANS tables
    down to filings from issuers in cik_set. Returns None if none matched
    (either the quarter has no filings for this universe, or a required table
    is missing/malformed)."""
    submission = _read_table(zf, "submission")
    if submission is None:
        logger.warning("SUBMISSION table not found in this quarter's ZIP")
        return None

    issuer_col = next((c for c in submission.columns if "issuer" in c and "cik" in c), None)
    if issuer_col is None:
        logger.warning("No issuer CIK column in SUBMISSION table: %s", submission.columns.tolist())
        return None

    submission[issuer_col] = pd.to_numeric(submission[issuer_col], errors="coerce")
    submission = submission[submission[issuer_col].isin(cik_set)].copy()
    if submission.empty:
        return None

    acc_col = "accession_number" if "accession_number" in submission.columns else submission.columns[0]
    accessions = set(submission[acc_col])

    transactions = _read_table(zf, "nonderiv_trans")
    if transactions is None:
        return None
    txn_acc_col = next((c for c in transactions.columns if "accession" in c), None)
    if txn_acc_col is None:
        return None
    transactions = transactions[transactions[txn_acc_col].isin(accessions)].copy()
    if transactions.empty:
        return None

    owners = _read_table(zf, "reportingowner")
    if owners is not None:
        owner_acc_col = next((c for c in owners.columns if "accession" in c), None)
        owners = owners[owners[owner_acc_col].isin(accessions)].copy() if owner_acc_col else pd.DataFrame()
    else:
        owners = pd.DataFrame()

    return {
        "submission": submission,
        "issuer_col": issuer_col,
        "acc_col": acc_col,
        "transactions": transactions,
        "txn_acc_col": txn_acc_col,
        "owners": owners,
    }
