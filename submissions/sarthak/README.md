# Form 4 Insider Transactions Pipeline

## Overview

Collects non-derivative insider-trading transactions (Form 4) for a
configured universe of companies from SEC EDGAR's official quarterly bulk
data sets, and produces a single clean CSV of transactions with issuer,
reporting-owner, and role information joined in.

## Setup

```bash
python -m venv venv
source venv/bin/activate       # Windows: venv\Scripts\activate
pip install -r pipeline/requirements.txt
cp pipeline/config.example.yaml pipeline/config.yaml
# edit pipeline/config.yaml: set sec_edgar.user_agent to a real, traceable
# contact per SEC's fair-access policy, and adjust paths/quarter range if needed
```

The universe file at `pipeline/universe/sp500_constituents.csv` (CIK + ticker
columns) ships with this submission as a small reference input — swap in a
different universe file and point `universe.input_csv` at it to collect a
different set of companies.

## Running the pipeline

```bash
cd pipeline
python main.py --config config.yaml
```

This iterates every quarter in the configured range (`run.start_year` /
`run.start_quarter` through `run.end_year` / `run.end_quarter`), downloads
that quarter's SEC bulk ZIP once, filters it to the configured universe,
cleans and enriches it, and appends the result to `paths.output_csv`.

To collect (or re-verify) just a small slice, e.g. for a quick check:

```bash
python main.py --config config.yaml --start-quarter 2024Q1 --end-quarter 2024Q1
```

## Resuming after interruption

After each quarter is fully processed and appended to the output CSV, its
label (e.g. `2024Q1`) is recorded in `paths.checkpoint_file` (a small JSON
file). If the pipeline is interrupted — killed, crashed, network failure —
simply re-run the same command: already-completed quarters are skipped, and
the run picks up at the next unprocessed quarter. No full re-run is ever
required.

## Output

- **Format:** CSV, one row per non-derivative insider transaction.
- **Location:** `paths.output_csv` in `config.yaml` (default `./data/output/form4_transactions.csv`).
- **Schema:** see `metadata.yaml` → `output.schema`. A 60-row sample of the
  real output shape is at `sample/form4_sample.csv` in this submission.
- **Full dataset:** not committed to this repository (per the integration
  guidelines). See `metadata.yaml` → `storage_policy` for the OneDrive link
  and access instructions.

## Known limitations

- Only **non-derivative** transactions are collected (the SEC `NONDERIV_TRANS`
  table). Derivative/options transactions are out of scope for this pipeline.
- A small fraction of rows (~0.18%) carry a `transaction_date` slightly
  outside their filing quarter — mostly late-December filings that appear in
  the following year's Q1 bulk file. This is a property of SEC's source data,
  not a bug in this pipeline.
- Owner role flags are derived from SEC's `rptowner_relationship` string
  encoding, confirmed present in every quarter from 2006 Q1 through 2024 Q1
  while building this pipeline. `pipeline/postprocess.py` also supports an
  older boolean-column encoding defensively, in case SEC reintroduces it.
- SEC EDGAR requires a real, traceable contact in the `User-Agent` header
  (fair-access policy, not authentication) — set this in `config.yaml`
  before running.
