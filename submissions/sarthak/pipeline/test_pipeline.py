"""
Smoke test for the Form 4 pipeline.

Exercises the actual collect.py / postprocess.py code (not a stand-in) against
one real SEC EDGAR quarter, filtered to a tiny hardcoded ticker subset, and
asserts the output has the expected schema and non-empty, sane-looking rows.

Run: python test_pipeline.py
Exit code is non-zero on failure, so this can be wired into CI.
"""
import sys

from collect import collect_quarter, fetch_quarter_zip
from postprocess import FINAL_COLUMNS, process_quarter

# A handful of well-known, high-filing-volume issuers — kept small so this
# test runs in seconds, not minutes.
TEST_UNIVERSE = {
    320193: "AAPL",
    789019: "MSFT",
    1318605: "TSLA",
}
TEST_QUARTER = (2024, 1)

SEC_CFG = {
    "url_template": "https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/{year}q{quarter}_form345.zip",
    "user_agent": "FinStreamAI Contributor <contact@example.com> (pipeline smoke test)",
    "request_timeout_sec": 120,
    "request_sleep_sec": 0.5,
    "max_retries": 3,
}


def run() -> bool:
    year, quarter = TEST_QUARTER
    print(f"[INFO] Fetching {year}Q{quarter} for {len(TEST_UNIVERSE)} test tickers...")
    zf = fetch_quarter_zip(SEC_CFG, year, quarter)

    raw = collect_quarter(zf, set(TEST_UNIVERSE.keys()))
    if raw is None:
        print("[FAIL] collect_quarter returned no data for a quarter known to have filings")
        return False
    print(f"[PASS] collect_quarter returned {len(raw['transactions'])} candidate transaction rows")

    cleaned = process_quarter(raw, TEST_UNIVERSE)

    if list(cleaned.columns) != FINAL_COLUMNS:
        print(f"[FAIL] unexpected columns: {cleaned.columns.tolist()}")
        return False
    print(f"[PASS] output has the expected {len(FINAL_COLUMNS)} columns")

    if cleaned.empty:
        print("[FAIL] no rows produced for a quarter/universe known to have filings")
        return False
    print(f"[PASS] {len(cleaned)} rows produced")

    if not cleaned["ticker"].isin(TEST_UNIVERSE.values()).all():
        print("[FAIL] found tickers outside the test universe — filtering is broken")
        return False
    print("[PASS] all rows belong to the test universe")

    if cleaned["transaction_date"].isna().all():
        print("[FAIL] transaction_date failed to parse for every row")
        return False
    sample_date = cleaned["transaction_date"].dropna().iloc[0]
    print(f"[PASS] transaction_date parses to ISO format, e.g. {sample_date}")

    print("\nSample rows:")
    preview_cols = ["ticker", "owner_name", "officer_title", "transaction_date", "transaction_code", "shares"]
    print(cleaned[preview_cols].head(5).to_string(index=False))
    return True


if __name__ == "__main__":
    ok = run()
    sys.exit(0 if ok else 1)
