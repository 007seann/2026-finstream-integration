# Fundamental Data Pipeline (EODHD)

## Overview
Fetches annual and quarterly fundamentals for every active S&P 500 constituent from the EODHD Fundamental Data API and writes them to MongoDB. Captures every numeric field EODHD reports per fiscal period (Income Statement, Balance Sheet, Cash Flow, EPS) rather than a fixed taxonomy, so nothing is silently discarded. Ticker universe comes from the platform's shared `companies` table (falls back to a local CSV -sp500_union_constituents.csv if that's unreachable).

## Setup
```bash
# macOS / Linux
python -m venv venv
source venv/bin/activate
pip install -r pipeline/requirements.txt
cp pipeline/.env.example pipeline/.env
cp pipeline/config.example.yaml pipeline/config.yaml
# fill in pipeline/.env with your EODHD API token + Postgres user/password (secrets)
# fill in pipeline/config.yaml with hosts/ports/paths/params (non-secret)
# neither file is committed
```

```powershell
# Windows (PowerShell)
python -m venv venv
venv\Scripts\Activate.ps1
# if this errors with an execution-policy message, run once:
#   Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
pip install -r pipeline/requirements.txt
copy pipeline\.env.example pipeline\.env
copy pipeline\config.example.yaml pipeline\config.yaml
# fill in pipeline/.env with your EODHD API token + Postgres user/password (secrets)
# fill in pipeline/config.yaml with hosts/ports/paths/params (non-secret)
# neither file is committed
```

## Running the pipeline
```bash
cd pipeline
python main.py
```

For a cheap first test before running the full ticker universe (each ticker costs 10 real EODHD API requests — see `metadata.yaml`), set `run.ticker_limit` in `config.yaml` to a small number like `3`.

## Resuming after interruption
Two layers of resumability:
- **Within a run**: a JSON checkpoint (`paths.checkpoint_file` in config, default `./data/.checkpoint.json`) records which tickers have already been fetched, saved after *each* ticker rather than only at the end. If the process is killed mid-run, re-running `python main.py` picks up exactly where it left off instead of re-fetching (and re-paying for) tickers already done.
- **Across runs / at the data layer**: every MongoDB write is an upsert keyed on `(ticker, year, report_type)` for annual records and `(ticker, year, quarter, report_type)` for quarterly records. Even a full re-run from scratch can never create a duplicate document.

The checkpoint resets automatically after a fully successful run, since this is a periodic refresh pipeline (re-pulling to catch newly published filings), not a one-shot backfill that should permanently skip completed tickers.

## Output
MongoDB `financial_db` database:
- `annual_fundamental` — one document per (ticker, fiscal year)
- `quarter_fundamental` — one document per (ticker, fiscal year, quarter)

Document shape and field-naming convention are documented in `metadata.yaml` under `output.schema`; a small illustrative sample is in `samples/sample_annual_fundamental.json`.

## Known limitations
- **Field names typed from EODHD's documentation, not exhaustively verified.** Most have been confirmed against a live API response; a few (basic vs. diluted EPS split, preferred-stock dividends specifically) have no clean 1:1 EODHD equivalent — flagged inline in `pipeline/fundamentals_mapper.py`'s docstring rather than guessed at.
- **Historical depth exceeds the comparison baseline used by prior fundamentals collection efforts on this project** (EODHD returns up to ~40 years of history for long-listed firms vs. the 15-year 2010–2024 window a prior WRDS/Compustat-based pipeline targeted) — kept at full depth by default since it costs nothing extra per API call; worth an explicit call if exact comparability with that prior window matters more than maximum depth.
- **Ticker universe depends on an external, shared table** (`companies`) being kept current by another part of the platform — if that table hasn't been refreshed recently, the fallback CSV (sp500_union_constituents.csv) may not reflect the very latest S&P 500 constituent changes either.
