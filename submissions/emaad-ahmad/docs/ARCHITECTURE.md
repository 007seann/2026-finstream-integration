# Architecture Overview

## Three-Layer ETL Pipeline

The platform is a fan-out / converge Airflow topology backed by a hybrid
PostgreSQL + MongoDB storage layer, all containerised via Docker Compose.

```
                   ┌──────────────────────────────────────────────┐
                   │   L0: sp500_refresh_pipeline  (weekly Mon)   │
                   │   Wikipedia → companies table                │
                   └──────────────────────────────────────────────┘
                                        │
                                        ▼
   ┌──────────────────┬────────────────────┬────────────────────┐
   │                  │                    │                    │
L1 │ eodhd_price      │ eodhd_intraday     │ eodhd_news         │  gdelt_news
   │ (overnight EOD)  │ (mkt hrs, 30-min)  │ (every 3 h)        │  (every 15 min)
   │  PostgreSQL      │ PostgreSQL         │ MongoDB            │  MongoDB
   │  price_data      │ price_data         │ news_articles      │  news_articles
   │  (interval=1d)   │ (interval=5m)      │ (source=eodhd)     │  (source=gdelt)
   └──────┬───────────┴──────────┬─────────┴─────────┬──────────┴────────┬──┘
          │                      │                   │                   │
          └───────────────┬──────┴───────────────────┴───────────────────┘
                          ▼
                   ┌──────────────────────────────────────────────┐
                L2 │ temporal_fusion_pipeline  (hourly)           │
                   │  entity_mapper.py  +  temporal_fusion.py     │
                   │    (spaCy NER; boilerplate stripper;         │
                   │     sliding window F(t) per interval)        │
                   │  → MongoDB fused_events                       │
                   └──────┬───────────────────────────────────────┘
                          ▼
                   ┌──────────────────────────────────────────────┐
                L3 │ sentiment_enrichment_pipeline  (hourly + 30) │
                   │  FinBERT + DistilRoBERTa  (Hugging Face)     │
                   │  → MongoDB sentiment_scores                   │
                   │  → enrich fused_events in-place              │
                   └──────┬───────────────────────────────────────┘
                          ▼
                   ┌──────────────────────────────────────────────┐
                   │ FastAPI serving layer  (9 REST endpoints)    │
                   │  /v1/prices, /v1/news, /v1/fused,            │
                   │  /v1/sentiment, /v1/stats, /v1/eodhd/usage   │
                   └──────────────────────────────────────────────┘
```

## DAG Catalogue

### Live DAGs (7)

| DAG                              | Layer | Production schedule       | Notes                                                                 |
|----------------------------------|:-----:|---------------------------|-----------------------------------------------------------------------|
| `sp500_refresh_pipeline`         | L0    | `0 4 * * 1`               | Weekly Wikipedia scrape; soft-deletes removed constituents.           |
| `eodhd_price_pipeline`           | L1    | `0 4 * * 2-6`             | Overnight EOD (after full vendor propagation, ~503 credits/day).      |
| `eodhd_intraday_pipeline`        | L1    | `*/30 13-20 * * 1-5`      | Intraday 5m during US market hours only; auto-skips off-hours.        |
| `eodhd_news_pipeline`            | L1    | `0 */3 * * *`             | Every 3 hours; ~2,515 credits per full-universe poll.                 |
| `gdelt_news_pipeline`            | L1    | `*/15 * * * *`            | Free; mirrors GDELT publish cadence.                                  |
| `temporal_fusion_pipeline`       | L2    | `@hourly`                 | Streaming per-ticker; ~28 min end-to-end on full S&P 500.             |
| `sentiment_enrichment_pipeline`  | L3    | `30 * * * *`              | Hourly, offset 30 min from fusion. Idempotent no-op if no new news.   |

### Historical / Paused DAGs (3)

Intended for one-shot use on the Azure VM after project handoff, to seed
the /data folder with deep history for research assistants. All three ship
with `is_paused_upon_creation=True` so they cannot accidentally trigger on
the dev environment.

| DAG                                    | Purpose                                      | Cost budget                                    |
|----------------------------------------|----------------------------------------------|------------------------------------------------|
| `eodhd_price_historical_pipeline`      | 20-year EOD backfill (all tickers, including soft-deleted). | ~503 credits per run (one-shot). |
| `eodhd_news_historical_pipeline`       | Deep-history news backfill via offset pagination.           | Up to ~125k credits worst case; use env-var chunking. |
| `gdelt_historical_pipeline`            | GDELT GKG 2.0 backfill from 18 Feb 2015 via masterfilelist. | Free; ~20 days runtime for full 10-year window. |

## Storage Rationale

**Why hybrid PostgreSQL + MongoDB rather than one store?**

- Price bars are strictly typed, range-queried on timestamp, and grow
  linearly (3.2M rows for a 4-month window at 5-minute intervals).
  PostgreSQL's covering B-tree indexes make range queries on this scale
  tractable at tens-of-milliseconds; storing this in MongoDB would forfeit
  index-only scans.
- News articles carry per-source variable metadata (EODHD has body text +
  vendor-scored sentiment; GDELT has themes + tone but no body). Storing
  these in a relational schema would force `TEXT` columns and JSON parsing
  on every fusion read. MongoDB's document model matches the source
  characteristics directly.
- Fused events are read one-ticker-one-interval at a time; MongoDB's
  primary-key upsert semantics match the fusion algorithm's per-ticker
  streaming pattern.

## Airflow Compatibility

- Target: Airflow 2.8.1 (FinStream production pin). Developed on 2.9.3.
- All DAGs use the TaskFlow API (`@dag` + `@task`), stable across 2.4+.
- `schedule=` keyword argument (not the older `schedule_interval=`) is used
  throughout, which is the canonical modern spelling supported in 2.8+.
- `is_paused_upon_creation=True` on the 3 historical DAGs is supported
  in 2.8+.
- `max_active_runs=1` on all schedulable DAGs is set to prevent overlapping
  runs on the production cadence.

## Python 3.8 Compatibility

- Every module uses `from __future__ import annotations`, so PEP-604 union
  types (`str | None`, `list[str]`) are deferred as strings at runtime and
  work on Python 3.8.
- No pattern matching (`match/case`), no walrus operator in expressions
  that would break, no `dict1 | dict2` runtime merges — all safe on 3.8.
- `requirements.txt` pins Python-3.8-compatible releases of pandas
  (2.0.x), lxml (5.2.2), transformers (4.36.2), torch (2.1.2).

## Rejected Alternatives

- **Pure PostgreSQL** — forces news content into TEXT columns; pays JSON
  parsing cost on every fusion read; loses upsert-by-`dedup_key` semantics
  that idempotency depends on.
- **Pure MongoDB** — loses covering B-tree indexes on timestamps; range
  queries over 3.2M price rows become expensive full-collection scans.
- **Apache Kafka streaming** — source data is not streaming: EODHD returns
  snapshot polls, GDELT publishes 15-min batches, Wikipedia refreshes weekly.
  Wrapping polled sources in a Kafka layer would add operational surface
  (broker, KRaft/Zookeeper, monitoring) without changing the underlying
  data-availability rate. Airflow's pull model matches the source
  characteristics.
