# Heterogeneous Real-Time Financial Data Pipeline

**Submission:** `emaad-ahmad`
**Author:** Emaad Ahmad (s2795901), MSc Data Science, University of Edinburgh
**Supervisor:** Dr Tiejun Ma
**Integration target:** FinStreamAI production stack (Azure VM `/data/test-2026`)

## Overview

A containerised, three-layer ETL pipeline that ingests heterogeneous
financial data (intraday OHLCV, ticker-tagged commercial news, open-data
event-graph metadata) from public APIs, temporally aligns the streams via
a sliding-window fusion algorithm, enriches records with dual-transformer
sentiment (FinBERT + RoBERTa), and serves the unified corpus via a
FastAPI REST layer. Designed as the open-data reproduction substrate for
the FININ and MANA-Net news-driven prediction frameworks published by the
supervisor's group.

- **10 Airflow DAGs** (7 live + 3 historical/paused-on-create)
- **Hybrid storage**: PostgreSQL 15 (structured OHLCV) + MongoDB 7 (documents)
- **9 FastAPI endpoints** for downstream consumers
- **Docker Compose** stack of 4 services (postgres, mongodb, airflow, fastapi)
- **Validated at full S&P 500 scale** (503 tickers, 3.21M price rows,
  3.21M fused events, 11,968 sentiment scores, 0 duplicate keys across
  6.44M records). See `docs/VALIDATION_EVIDENCE.md`.

## Setup

```bash
# 1. Configure secrets (real .env is gitignored — never commit it)
cp pipeline/.env.example pipeline/.env
# Fill in at minimum:
#   POSTGRES_PASSWORD, MONGO_INITDB_ROOT_PASSWORD, EODHD_API_TOKEN
#   (EODHD_FREE_TIER=true for the free tier; false for the paid all-in-one plan)

# 2. Bring up the stack (postgres + mongodb + airflow + fastapi)
cd pipeline
docker compose up -d

# 3. First-time bootstrap: seed the S&P 500 constituents table
#    (Wait ~3-5 min for Airflow to finish first-boot pip install.)
docker exec finplatform_airflow airflow dags trigger sp500_refresh_pipeline
```

The Airflow UI is at http://localhost:8080 (admin / admin) and the FastAPI
docs are at http://localhost:8000/docs once the stack is running.

## Secrets and the `.env` file

The EODHD API token, PostgreSQL / MongoDB passwords, and other environment
variables live in a `.env` file next to `docker-compose.yml`. This file is
**never committed** — the repo's `.gitignore` explicitly excludes `.env`,
`credentials.txt`, `*.key`, and `*.pem`. Only `.env.example` (with
placeholder values) is committed.

| Environment | Where `.env` lives | Committed? |
|---|---|:---:|
| Local dev machine | `pipeline/.env` (next to `docker-compose.yml`) | ❌ gitignored |
| **Azure VM (post-deployment)** | `/data/test-2026/pipeline/.env` on the VM (RA team provisions once using Prof Ma's shared paid EODHD token) | ❌ never on the VM's git tree |
| This fork of the integration repo | `pipeline/.env.example` only (placeholder values) | ✅ example only |

Docker Compose reads `${EODHD_API_TOKEN}` via variable substitution at
runtime, so the same mechanism works identically local ↔ VM — nothing in
the code changes.

## Production cadence — how the DAGs are wired

Every schedulable DAG in this submission has its **production cron already
set** in its `@dag(schedule=...)` decorator. Nothing needs to be edited
by hand at deployment. However, **every DAG lands paused** in Airflow
because `docker-compose.yml` sets
`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true`. This is deliberate —
it lets the RA team stand up the stack, verify Airflow can parse every
DAG and reach every dependency, and only then explicitly unpause the
live DAGs one at a time.

| DAG                              | Wired schedule            | Auto-runs after `airflow dags unpause`? |
|----------------------------------|---------------------------|:---:|
| `sp500_refresh_pipeline`         | `0 4 * * 1`               | ✅ yes |
| `eodhd_price_pipeline`           | `0 4 * * 2-6`             | ✅ yes |
| `eodhd_intraday_pipeline`        | `*/30 13-20 * * 1-5`      | ✅ yes |
| `eodhd_news_pipeline`            | `0 */3 * * *`             | ✅ yes |
| `gdelt_news_pipeline`            | `*/15 * * * *`            | ✅ yes |
| `temporal_fusion_pipeline`       | `@hourly`                 | ✅ yes |
| `sentiment_enrichment_pipeline`  | `30 * * * *`              | ✅ yes |
| `eodhd_price_historical_pipeline`| **None** (`is_paused_upon_creation=True`) | ❌ manual trigger only |
| `eodhd_news_historical_pipeline` | **None** (`is_paused_upon_creation=True`) | ❌ manual trigger only |
| `gdelt_historical_pipeline`      | **None** (`is_paused_upon_creation=True`) | ❌ manual trigger only |

To activate all 7 live DAGs on the VM after deployment:

```bash
for dag in sp500_refresh_pipeline eodhd_price_pipeline eodhd_intraday_pipeline \
           eodhd_news_pipeline gdelt_news_pipeline temporal_fusion_pipeline \
           sentiment_enrichment_pipeline; do
  docker exec finplatform_airflow airflow dags unpause "$dag"
done
```

The 3 historical DAGs remain paused. Trigger them one-shot manually only
when you're ready to seed the deep-history corpus for research assistants.

**Local prod-cadence testing is intentionally not part of this submission
workflow.** Pipeline correctness was validated on 8 July 2026 via manual
Cycle 1/2 triggers (see `docs/VALIDATION_EVIDENCE.md`); running the live
crons locally would consume EODHD credits daily without adding evidence
value, and dev machines that sleep/reboot would produce spurious
"missed schedule" false alarms in Airflow. The Azure VM (always-on,
always-connected) is the correct environment for prod-cadence validation.

## Running the pipeline

Two equivalent options:

```bash
# Option A: dispatcher script (thin wrapper around `airflow dags trigger`)
python pipeline/main.py --stage L1         # ingestion (price + news + gdelt)
python pipeline/main.py --stage L2         # temporal fusion
python pipeline/main.py --stage L3         # transformer sentiment
python pipeline/main.py --stage all        # L0 -> L1 -> L2 -> L3 end-to-end
python pipeline/main.py --verify           # print /v1/stats snapshot

# Option B: trigger DAGs directly
docker exec finplatform_airflow airflow dags trigger eodhd_price_pipeline
docker exec finplatform_airflow airflow dags trigger eodhd_news_pipeline
docker exec finplatform_airflow airflow dags trigger gdelt_news_pipeline
docker exec finplatform_airflow airflow dags trigger temporal_fusion_pipeline
docker exec finplatform_airflow airflow dags trigger sentiment_enrichment_pipeline
```

Full details of each DAG's role live in `docs/ARCHITECTURE.md`.

## Resuming after interruption

Every DAG is **fully idempotent** by design:

- **PostgreSQL `price_data`**: primary key `(ticker, timestamp_ms, interval)`
  with `ON CONFLICT DO NOTHING`. Re-triggering the price DAG re-fetches the
  same 30-day rolling window but inserts only new bars.
- **MongoDB `news_articles`, `fused_events`, `sentiment_scores`**: upsert on
  a composite `dedup_key`. Repeat runs cannot create duplicates.
- **Airflow**: `max_active_runs=1` on every schedulable DAG prevents
  overlapping runs, and a `retries=1..2` default in each DAG's
  `default_args` handles transient API errors.
- **Cycle 2 validation on 8 July 2026** proved the invariant: repeat-polling
  the entire pipeline added rows for genuinely new data only, and produced
  **0 duplicate keys across 6.44M records** on four dedup surfaces.

To resume from a failed task, either wait for Airflow's built-in retry or
manually:

```bash
docker exec finplatform_airflow airflow tasks clear \
    <dag_id> --task-ids <task_id> --start-date <run_date>
```

## Output

Data lands in two stores inside the Docker network:

| Layer | Store | Collection / Table | Read via |
|---|---|---|---|
| L1 Prices | PostgreSQL | `price_data` (interval 1d + 5m) | `GET /v1/prices?ticker=...` |
| L1 News  | MongoDB | `news_articles` (`source: "eodhd"` or `"gdelt"`) | `GET /v1/news?ticker=...` |
| L2 Fused | MongoDB | `fused_events` | `GET /v1/fused?ticker=...` |
| L3 Sentiment | MongoDB | `sentiment_scores` (per (article, model) pair) | `GET /v1/sentiment?ticker=...&model=...` |
| Ops     | Both    | via FastAPI aggregation | `GET /v1/stats`, `GET /v1/eodhd/usage` |

Full schema in `metadata.yaml` (`output.schema`). Illustrative small samples
of each collection are in `samples/` for reviewer shape-check.

## Known limitations

- **NULL OHLCV rows**: approximately 1.4% of 5-minute bars carry a NULL
  volume field for the final bar of the trading day. This is an EODHD
  end-of-day consolidation artefact, not a pipeline defect. Downstream
  code should filter `WHERE volume IS NOT NULL`.
- **Cross-source URL overlap**: EODHD and GDELT share only 0.07% of URLs,
  so the strict four-way sentiment agreement is effectively undefined at
  the article level. Aggregate ticker-day agreement is used instead (see
  dissertation Chapter 4 §4.3).
- **GDELT ticker coverage bias**: 1,982 GDELT-attached fused events span
  ~90 tickers with a strong Pareto shape (top 5 mega-caps dominate).
  Approximately 410 of 503 active constituents receive zero GDELT
  attachment during a typical observation window. This is a source-side
  characteristic, not a bug.
- **Historical DAGs are paused-on-create**: `eodhd_price_historical_pipeline`,
  `eodhd_news_historical_pipeline`, and `gdelt_historical_pipeline` are
  intentionally paused. They are intended for one-shot backfill on the
  Azure VM only, after project handoff.
- **SEC EDGAR ingestion disabled**: SEC 10-K/10-Q filings were scoped out
  of this iteration (2026-05-28). The `sec_edgar_dag.py` file exists in
  the source repository but is intentionally omitted from this submission.
- **Data storage on the Azure VM**: the raw EODHD payload is subject to
  EODHD's ToS (no third-party redistribution). Anything derived
  (aggregates, evaluation metrics, plots) is unrestricted; raw responses
  live in Docker volumes only. GDELT and Wikipedia derivatives are
  unrestricted.

## Documentation

- `metadata.yaml` — data source metadata (endpoints, auth, rate limits, schema).
- `docs/ARCHITECTURE.md` — three-layer pipeline overview and DAG catalogue.
- `docs/VALIDATION_EVIDENCE.md` — Cycle 1 & 2 empirical validation summary.
- `docs/phase1_evidence_2026-07-08/` — raw milestone snapshots from validation day.
- `samples/` — small JSON samples for reviewer shape-check.

## Contact

For questions about this submission:
- **Emaad Ahmad** (author) — emaad.ahmad@ed.ac.uk (s2795901)
- **Dr Tiejun Ma** (supervisor)
- **Sean Choi / Sarah** (RA integration reviewers, via PR discussion)
