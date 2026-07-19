# Empirical Validation Evidence — 8 July 2026

Two back-to-back validation cycles were run against the full S&P 500
universe on the shared paid EODHD account. Cycle 1 established the
baseline corpus; Cycle 2 (~2 hours later) proved idempotency and
verified the market-hours gate. All numbers below are drawn from the
audit snapshots preserved under `phase1_evidence_2026-07-08/` in this
folder.

## Headline Numbers

| Metric | Value | Source |
|---|---:|---|
| Records validated across 4 dedup surfaces | 6,442,403 | Cycle 2 audit |
| Duplicate keys observed | **0** | Cycle 2 audit |
| Price rows ingested | 3,211,294 | `/v1/stats` at 21:00 UTC |
| News articles ingested (EODHD + GDELT) | 8,866 | `/v1/stats` |
| Fused events produced | 3,210,774 | `/v1/stats` |
| Sentiment scores produced (2 models × articles) | 11,968 | `/v1/stats` |
| Active S&P 500 constituents polled | 503 / 503 (100%) | `companies_active` |
| EODHD credits consumed on 8 July | 17,615 / 100,000 (17.6%) | `/v1/eodhd/usage` |
| Full end-to-end cycle runtime | 63 minutes | Airflow DAG timings |
| GDELT-attached fused events | 1,982 across ~90 tickers | Fusion audit |
| EODHD-attached fused events | 57,841 | Fusion audit |
| Market-hours gate delta at 20:53 UTC | Δprice_5m = 0 | Post-close control run |
| Idempotent no-op runtime (sentiment DAG, 20:46 UTC) | 4 seconds | Airflow logs |

## Dedup Validation (Cycle 2, 21:00 UTC)

| Store | Records | Duplicate keys |
|---|---:|---:|
| PostgreSQL `price_data` (1d) | 12,294 | **0** |
| PostgreSQL `price_data` (5m) | 3,199,000 | **0** |
| MongoDB `news_articles` | 8,866 | **0** |
| MongoDB `fused_events` | 3,210,774 | **0** |
| MongoDB `sentiment_scores` | 11,968 | **0** |
| **Total** | **6,442,403** | **0** |

## Sentiment Cross-Validation (RQ3)

| Model | Records | Agree with EODHD | Agreement % |
|---|---:|---:|---:|
| FinBERT (`ProsusAI/finbert`) | 5,602 | 3,317 | **59.2%** |
| RoBERTa (`mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis`) | 5,602 | 2,670 | **47.7%** |
| Aggregate | 11,204 | 5,987 | **53.5%** |

**Gap:** 11.5 percentage points between FinBERT and RoBERTa. Stable across
corpus sizes (11.9 pp on the earlier 4,989-record snapshot). Divergence
concentrates on the positive-vs-neutral boundary — RoBERTa relabels
approximately 1 in 9 of FinBERT's "neutral" articles as "positive".

## Two Bugs Fixed During Validation

Both fixed the same day; final numbers above are post-fix.

1. **Price DAG XCom OOM.** `fetch_intraday_prices` accumulated ~40k dicts
   in memory before returning them via XCom → SIGKILL after 15 min on the
   503-ticker universe. Fix: per-ticker `insert_prices()` immediately,
   return integer counts only.

2. **Fusion DAG XCom OOM + `DEMO_TICKERS` bug.** `load_prices` tried to
   materialise all 3.2M rows as a list of dicts (OOM); `run_fusion`
   iterated only the 10 `DEMO_TICKERS` (pre-existing scope defect).
   Combined rewrite: per-ticker streaming across all 503 active
   constituents. Fusion corpus grew from **6,842 → 3,210,774** events
   (469×), GDELT-attached from **17 → 1,982** (117×).

## Decision Gate — Phase 2 GO

| # | Check | Result |
|:---:|---|:---:|
| 1 | All Cycle 2 DAG runs succeeded | ✅ 6/6 |
| 2 | PostgreSQL price dedup | ✅ 0 / 3.21M |
| 3 | MongoDB news dedup | ✅ 0 / 8,866 |
| 4 | MongoDB fusion dedup | ✅ 0 / 3.21M |
| 5 | MongoDB sentiment dedup | ✅ 0 / 11,968 |
| 6 | Row-delta bounded to new data | ✅ +499 prices, +784 news, +762 scores |
| 7 | End-to-end runtime ≤ 70 min | ✅ 63 min |
| 8 | Market-hours gate observed | ✅ intraday Δ = 0 at 20:53 UTC |
| 9 | Total credit spend | ✅ 17,615 / 100,000 (17.6%) |
| 10 | Idempotent no-op verified | ✅ 20:46 sentiment run = 4 sec |

## Evidence Artefacts (in `phase1_evidence_2026-07-08/`)

- `PHASE1_COMPLETE.txt` — top-line milestone summary
- `CYCLE2_MILESTONE.txt` — Cycle 2 completion marker
- `cycle1_milestone.txt` — Cycle 1 headline numbers
- `fusion_milestone.txt` — post-fix fusion milestone snapshot

Full validation report available in the source repository at
`MSc Dissertation/status-reports/cycle_validation_2026-07-08.md`.
