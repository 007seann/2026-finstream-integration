"""
Heterogeneous Real-Time Financial Data Pipeline
===============================================
Dispatcher entrypoint for the FinStreamAI integration submission.

This project is an Apache Airflow application that runs as a 4-service Docker
Compose stack (postgres, mongodb, airflow, fastapi). The template's
`collect() / process() / save()` pattern does not map cleanly to a
production DAG orchestration architecture, so this main.py acts as a thin
DISPATCHER that documents each stage and shells out to the Airflow CLI to
trigger the corresponding DAG. Checkpoint/resume semantics are provided
natively by Airflow (max_active_runs=1, ON CONFLICT DO NOTHING on the
PostgreSQL side, upsert-by-dedup_key on the MongoDB side) rather than by
a local JSON checkpoint file.

Usage
-----
    python main.py --stage L1        # ingestion (price + news + gdelt)
    python main.py --stage L2        # temporal fusion
    python main.py --stage L3        # transformer sentiment scoring
    python main.py --stage all       # L1 -> L2 -> L3 end-to-end
    python main.py --verify          # print /v1/stats snapshot

Prerequisites
-------------
The Docker Compose stack must be up (`docker compose up -d`) and Airflow
must have finished its first-boot pip install (~3-5 minutes on first run).
See README.md for full bootstrap steps.

Resume semantics
----------------
Every DAG is idempotent:
  * PostgreSQL price_data: ON CONFLICT (ticker, timestamp_ms, interval) DO NOTHING
  * MongoDB news_articles / fused_events / sentiment_scores: upsert on dedup_key
  * Airflow itself: max_active_runs=1 prevents overlapping runs; failed tasks
    can be re-triggered via `docker exec finplatform_airflow airflow tasks
    clear <dag_id> <task_id> <run_id>` and Airflow will retry cleanly.

Empirical validation is documented in ../docs/VALIDATION_EVIDENCE.md
(6.4M records validated across 4 dedup surfaces, 0 duplicates).
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


AIRFLOW_CONTAINER = "finplatform_airflow"
FASTAPI_URL = "http://localhost:8000"


# The 4 always-on DAGs. The 3 historical DAGs are intentionally omitted from
# this dispatcher because they are paused-on-create for Azure VM one-shot use
# (see docs/ARCHITECTURE.md for the rationale).
STAGE_DAGS: dict = {
    "L0": ["sp500_refresh_pipeline"],
    "L1": ["eodhd_price_pipeline", "eodhd_intraday_pipeline",
           "eodhd_news_pipeline",  "gdelt_news_pipeline"],
    "L2": ["temporal_fusion_pipeline"],
    "L3": ["sentiment_enrichment_pipeline"],
}


def _trigger_dag(dag_id: str) -> None:
    """Trigger a single Airflow DAG via docker exec."""
    cmd = ["docker", "exec", AIRFLOW_CONTAINER, "airflow", "dags", "trigger", dag_id]
    logger.info("Triggering %s", dag_id)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error("Trigger failed for %s: %s", dag_id, result.stderr.strip())
        raise RuntimeError(f"airflow dags trigger {dag_id} failed")
    logger.info("Trigger accepted for %s", dag_id)


def run_stage(stage: str) -> None:
    """Trigger every DAG belonging to the requested stage."""
    if stage not in STAGE_DAGS:
        raise ValueError(f"Unknown stage {stage!r}. Expected one of {list(STAGE_DAGS)}")
    for dag_id in STAGE_DAGS[stage]:
        _trigger_dag(dag_id)


def run_all() -> None:
    """
    Trigger L1 -> L2 -> L3 in dependency order.
    Note: Airflow does NOT wait for a DAG to finish before scheduling the
    next one. Downstream stages will re-run only on data that landed by
    the time they execute; because every DAG is idempotent this is safe
    but not immediate. For true sequential execution across DAGs use
    the Airflow ExternalTaskSensor or run this script three times with
    intervening polls of `airflow dags list-runs`.
    """
    logger.info("Running full pipeline (L0 maintenance + L1 -> L2 -> L3)")
    for stage in ("L0", "L1", "L2", "L3"):
        logger.info("--- Stage %s ---", stage)
        run_stage(stage)


def verify() -> None:
    """Print the FastAPI /v1/stats snapshot for a quick corpus health check."""
    import urllib.request
    import json
    try:
        with urllib.request.urlopen(f"{FASTAPI_URL}/v1/stats", timeout=10) as resp:
            payload = json.load(resp)
        print(json.dumps(payload, indent=2))
    except Exception as e:
        logger.error("Could not reach FastAPI at %s: %s", FASTAPI_URL, e)
        logger.error("Is the stack up? Try: docker compose up -d")
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--stage", choices=list(STAGE_DAGS) + ["all"],
                       help="Which layer to run.")
    group.add_argument("--verify", action="store_true",
                       help="Print the /v1/stats corpus snapshot.")
    args = parser.parse_args()

    if args.verify:
        verify()
    elif args.stage == "all":
        run_all()
    else:
        run_stage(args.stage)


if __name__ == "__main__":
    main()
