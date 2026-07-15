"""
Sentiment Scoring Engine (RQ3)
===============================
Runs transformer-based sentiment inference on financial text.

Models:
  1. FinBERT (ProsusAI/finbert) — primary, domain-specific
  2. DistilRoBERTa-financial  — secondary, cross-validation
  3. MiniLM (sentence-transformers) — fallback on SLA breach (>5min)

Each article gets scored by both FinBERT and RoBERTa.
Scores are compared against EODHD's pre-computed sentiment.
Results stored in MongoDB sentiment_scores collection.

References:
    IPP §3.3: FinBERT/RoBERTa for contextual sentiment
    RQ3: Will transformer sentiment improve forecasting
         over price-only baselines?
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from common.constants import (
    FINBERT_MODEL,
    ROBERTA_MODEL,
    SENTIMENT_SLA_SECONDS,
    SENTIMENT_BATCH_SIZE,
)

logger = logging.getLogger(__name__)

# Lazy-loaded pipelines
_pipelines = {}


def _get_pipeline(model_name: str):
    """Load a HuggingFace sentiment pipeline (lazy, cached)."""
    if model_name not in _pipelines:
        from transformers import pipeline as hf_pipeline

        logger.info(f"Loading model: {model_name} (first run downloads ~400MB)")
        start = time.time()

        _pipelines[model_name] = hf_pipeline(
            "sentiment-analysis",
            model=model_name,
            tokenizer=model_name,
            truncation=True,
            max_length=512,
            device=-1,  # CPU
        )

        elapsed = time.time() - start
        logger.info(f"Model {model_name} loaded in {elapsed:.1f}s")

    return _pipelines[model_name]


def _normalize_label(label: str, model_name: str) -> str:
    """Normalize sentiment labels across different models to {positive, negative, neutral}."""
    label_lower = label.lower()

    if model_name == FINBERT_MODEL:
        # FinBERT outputs: positive, negative, neutral
        return label_lower

    if model_name == ROBERTA_MODEL:
        # DistilRoBERTa-financial outputs: positive, negative, neutral
        return label_lower

    # Generic fallback
    if "pos" in label_lower:
        return "positive"
    elif "neg" in label_lower:
        return "negative"
    else:
        return "neutral"


def score_text(text: str, model_name: str = FINBERT_MODEL) -> dict:
    """
    Score a single text with a sentiment model.

    Returns:
        {
            "model": "ProsusAI/finbert",
            "label": "positive",
            "confidence": 0.94,
            "raw_label": "positive",
            "raw_score": 0.94,
        }
    """
    pipe = _get_pipeline(model_name)
    result = pipe(text[:512])[0]  # Truncate + get first result

    return {
        "model": model_name,
        "label": _normalize_label(result["label"], model_name),
        "confidence": round(result["score"], 4),
        "raw_label": result["label"],
        "raw_score": round(result["score"], 4),
    }


def score_batch(
    texts: list[str],
    model_name: str = FINBERT_MODEL,
    batch_size: int = SENTIMENT_BATCH_SIZE,
) -> list[dict]:
    """
    Score a batch of texts. Monitors SLA (5-min max).

    Returns:
        list of score dicts (same format as score_text)
    """
    pipe = _get_pipeline(model_name)
    start = time.time()

    results = []
    for i in range(0, len(texts), batch_size):
        # SLA check
        elapsed = time.time() - start
        if elapsed > SENTIMENT_SLA_SECONDS:
            logger.warning(
                f"SLA breach: {elapsed:.0f}s > {SENTIMENT_SLA_SECONDS}s after {len(results)} items. "
                f"Remaining {len(texts) - i} items skipped."
            )
            # Fill remaining with None
            for _ in range(len(texts) - i):
                results.append(None)
            break

        batch = [t[:512] for t in texts[i : i + batch_size]]
        batch_results = pipe(batch)

        for r in batch_results:
            results.append({
                "model": model_name,
                "label": _normalize_label(r["label"], model_name),
                "confidence": round(r["score"], 4),
                "raw_label": r["label"],
                "raw_score": round(r["score"], 4),
            })

    elapsed = time.time() - start
    logger.info(f"Scored {len(results)} texts with {model_name} in {elapsed:.1f}s")
    return results


def score_article(article: dict, models: list[str] = None) -> list[dict]:
    """
    Score a single news article with multiple models.

    Uses the article title as primary input (most informative,
    within model's context window).

    Args:
        article: MongoDB news article document
        models:  list of model names (defaults to FinBERT + RoBERTa)

    Returns:
        list of sentiment score documents ready for MongoDB
    """
    if models is None:
        models = [FINBERT_MODEL, ROBERTA_MODEL]

    title = article.get("title", "")
    if not title:
        return []

    ticker = article.get("ticker", "unknown")
    now = datetime.now(timezone.utc).isoformat()

    scores = []
    for model_name in models:
        try:
            result = score_text(title, model_name)
            score_doc = {
                "ticker": ticker,
                "source_type": "news",
                "source_dedup_key": article.get("dedup_key"),
                "text_scored": title,
                "model": model_name,
                "label": result["label"],
                "confidence": result["confidence"],
                "raw_label": result["raw_label"],
                "raw_score": result["raw_score"],
                "scored_at": now,
                "dedup_key": f"{article.get('dedup_key', '')}|{model_name}",
            }

            # Compare with EODHD sentiment if available
            eodhd_sent = article.get("eodhd_sentiment")
            if eodhd_sent:
                score_doc["eodhd_comparison"] = {
                    "eodhd_polarity": eodhd_sent.get("polarity"),
                    "eodhd_pos": eodhd_sent.get("pos"),
                    "eodhd_neg": eodhd_sent.get("neg"),
                    "eodhd_neu": eodhd_sent.get("neu"),
                    "agreement": _check_agreement(result, _eodhd_label(eodhd_sent)),
                }

            # Compare with GDELT tone if available
            gdelt_tone = article.get("gdelt_tone")
            if gdelt_tone:
                score_doc["gdelt_comparison"] = {
                    "gdelt_tone_score": gdelt_tone.get("tone_score"),
                    "gdelt_polarity":   gdelt_tone.get("polarity"),
                    "gdelt_label":      _gdelt_label(gdelt_tone),
                    "agreement":        _check_agreement(result, _gdelt_label(gdelt_tone)),
                }

            scores.append(score_doc)
        except Exception as e:
            logger.error(f"Error scoring with {model_name}: {e}")

    return scores


def _check_agreement(transformer_result: dict, other_label: str | None) -> bool:
    """Check if transformer label agrees with another model's label."""
    if other_label is None:
        return False
    return transformer_result["label"] == other_label


def _eodhd_label(eodhd_sentiment: dict) -> str | None:
    """Reduce EODHD's pos/neg/neu probabilities to a single dominant label."""
    if not eodhd_sentiment:
        return None
    eodhd_pos = eodhd_sentiment.get("pos", 0) or 0
    eodhd_neg = eodhd_sentiment.get("neg", 0) or 0
    eodhd_neu = eodhd_sentiment.get("neu", 0) or 0
    return max(
        [("positive", eodhd_pos), ("negative", eodhd_neg), ("neutral", eodhd_neu)],
        key=lambda x: x[1],
    )[0]


# GDELT tone thresholds — empirically chosen.
# GDELT's V1.5 `tone_score` = positive_density - negative_density, in -100..+100
# but typical financial-news values fall within -10..+10.
GDELT_POSITIVE_THRESHOLD = 1.5
GDELT_NEGATIVE_THRESHOLD = -1.5


def _gdelt_label(gdelt_tone: dict) -> str | None:
    """Reduce GDELT's tone_score to a {positive, neutral, negative} label."""
    if not gdelt_tone:
        return None
    score = gdelt_tone.get("tone_score")
    if score is None:
        return None
    if score >= GDELT_POSITIVE_THRESHOLD:
        return "positive"
    if score <= GDELT_NEGATIVE_THRESHOLD:
        return "negative"
    return "neutral"


def four_way_agreement(article: dict, finbert_result: dict, roberta_result: dict) -> dict:
    """
    Compute the 4-way agreement matrix for a single article across:
      FinBERT (domain-adapted transformer)
      RoBERTa (general-purpose transformer)
      EODHD pre-scored sentiment (commercial API)
      GDELT V1.5 tone (open-data, computed via TABARI/CAMEO pipeline)

    Returns:
        {
            "labels": {"finbert": ..., "roberta": ..., "eodhd": ..., "gdelt": ...},
            "all_agree": bool,
            "transformers_agree": bool,         finbert == roberta
            "commercial_vs_open_agree": bool,   eodhd == gdelt
            "modal_label": "positive" | "neutral" | "negative" | None,
            "vote_counts": {"positive": int, "neutral": int, "negative": int},
        }

    Useful for downstream analysis: which sources tend to agree?
    Where does the commercial vs open-data divergence lie?
    """
    labels = {
        "finbert": finbert_result.get("label") if finbert_result else None,
        "roberta": roberta_result.get("label") if roberta_result else None,
        "eodhd":   _eodhd_label(article.get("eodhd_sentiment")),
        "gdelt":   _gdelt_label(article.get("gdelt_tone")),
    }
    present = [v for v in labels.values() if v is not None]
    vote_counts = {"positive": 0, "neutral": 0, "negative": 0}
    for v in present:
        if v in vote_counts:
            vote_counts[v] += 1

    modal = None
    if present:
        modal = max(vote_counts.items(), key=lambda kv: kv[1])[0]
        if vote_counts[modal] == 0:
            modal = None

    return {
        "labels": labels,
        "all_agree": len(set(present)) == 1 and len(present) > 1,
        "transformers_agree": (
            labels["finbert"] is not None
            and labels["finbert"] == labels["roberta"]
        ),
        "commercial_vs_open_agree": (
            labels["eodhd"] is not None
            and labels["eodhd"] == labels["gdelt"]
        ),
        "modal_label": modal,
        "vote_counts": vote_counts,
    }
