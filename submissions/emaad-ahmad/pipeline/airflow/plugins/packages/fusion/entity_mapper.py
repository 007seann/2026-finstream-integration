"""
Entity Mapper (RQ2 - Step 1)
=============================
Extracts company entities from news text using spaCy NER,
maps them to ticker symbols, and scores relevance.

Solves the "Benzinga boilerplate" problem: most EODHD news articles
mention AAPL/TSLA in footer boilerplate, inflating false associations.

Pipeline:
  1. Strip boilerplate paragraphs (ads, footers, disclaimers)
  2. spaCy NER: extract ORG entities from clean text
  3. Map ORGs -> tickers via company name lookup
  4. Score relevance: title_match(0.5) + body_match(0.3) + eodhd_tag(0.2)
  5. Return only associations above threshold

References:
  IPP §3.2: E: org -> {s1, s2, ..., sk} ⊆ S
"""

from __future__ import annotations

import logging
import re
from typing import Optional

import spacy

from common.constants import BOILERPLATE_PATTERNS, ENTITY_RELEVANCE_THRESHOLD

logger = logging.getLogger(__name__)

# Lazy-loaded spaCy model
_nlp = None


def _get_nlp():
    """Load spaCy model once (lazy singleton)."""
    global _nlp
    if _nlp is None:
        _nlp = spacy.load("en_core_web_sm")
        logger.info("Loaded spaCy en_core_web_sm model")
    return _nlp


def strip_boilerplate(text: str) -> str:
    """
    Remove boilerplate paragraphs from article text.
    Benzinga, Motley Fool, etc. add stock tickers in footers
    that pollute entity extraction.
    """
    if not text:
        return ""

    paragraphs = text.split("\n")
    clean = []
    for para in paragraphs:
        para_stripped = para.strip()
        if not para_stripped:
            continue
        # Skip paragraphs that match boilerplate patterns
        if any(bp.lower() in para_stripped.lower() for bp in BOILERPLATE_PATTERNS):
            continue
        clean.append(para_stripped)

    return "\n".join(clean)


def extract_orgs(text: str) -> list[str]:
    """Extract ORG entities from text using spaCy NER."""
    nlp = _get_nlp()
    doc = nlp(text[:100000])  # Cap at 100k chars for memory safety
    orgs = []
    for ent in doc.ents:
        if ent.label_ == "ORG":
            # Normalize: strip Inc., Corp., Ltd., Co.
            name = re.sub(r"\s*(Inc\.?|Corp\.?|Ltd\.?|Co\.?|LLC|PLC)\s*$", "", ent.text).strip()
            if len(name) >= 2:
                orgs.append(name)
    return list(set(orgs))  # Deduplicate


def build_company_lookup(companies: list[dict]) -> dict:
    """
    Build a lookup dict from company names to tickers.
    Handles common variations (e.g., "Apple" matches "Apple Inc.").

    Args:
        companies: list of {ticker, company_name} dicts from PostgreSQL

    Returns:
        dict mapping normalized names -> ticker
    """
    lookup = {}
    for c in companies:
        ticker = c["ticker"].upper()
        name = c.get("company_name", "")
        if not name:
            continue

        # Exact name
        lookup[name.lower()] = ticker

        # Without suffix
        clean = re.sub(r"\s*(Inc\.?|Corp\.?|Ltd\.?|Co\.?|LLC|PLC|,?\s*Inc\.?)\s*$", "", name).strip()
        lookup[clean.lower()] = ticker

        # First word (for "Apple", "Microsoft", "Tesla", etc.)
        # Only if it's distinctive enough (>3 chars)
        first_word = clean.split()[0] if clean else ""
        if len(first_word) > 3:
            lookup[first_word.lower()] = ticker

        # Ticker itself as a match
        lookup[ticker.lower()] = ticker

    return lookup


def map_entities_to_tickers(
    orgs: list[str],
    company_lookup: dict,
) -> list[dict]:
    """
    Map extracted ORG entities to ticker symbols.

    Returns:
        list of {org, ticker, match_type} dicts
    """
    matches = []
    seen_tickers = set()

    for org in orgs:
        org_lower = org.lower()

        # Try exact match
        if org_lower in company_lookup:
            ticker = company_lookup[org_lower]
            if ticker not in seen_tickers:
                matches.append({"org": org, "ticker": ticker, "match_type": "exact"})
                seen_tickers.add(ticker)
            continue

        # Try partial match (org is substring of a known name)
        for known_name, ticker in company_lookup.items():
            if ticker in seen_tickers:
                continue
            if org_lower in known_name or known_name in org_lower:
                matches.append({"org": org, "ticker": ticker, "match_type": "partial"})
                seen_tickers.add(ticker)
                break

    return matches


def score_relevance(
    article: dict,
    ticker: str,
    company_lookup: dict,
    ner_matches: list[dict],
) -> float:
    """
    Score how relevant an article is to a specific ticker.

    Scoring (max 1.0):
      - 0.50: ticker's company name appears in title
      - 0.30: ticker's company name found by NER in cleaned body
      - 0.20: ticker in EODHD's symbols_mentioned list

    A relevance >= ENTITY_RELEVANCE_THRESHOLD keeps the association.
    """
    score = 0.0
    title = (article.get("title") or "").lower()
    symbols = [s.replace(".US", "").upper() for s in (article.get("symbols_mentioned") or [])]

    # Find company name variants for this ticker
    ticker_names = [name for name, t in company_lookup.items() if t == ticker]

    # Title mention (strongest signal)
    if any(name in title for name in ticker_names) or ticker.lower() in title:
        score += 0.50

    # NER body match
    if any(m["ticker"] == ticker for m in ner_matches):
        score += 0.30

    # EODHD tag
    if ticker in symbols:
        score += 0.20

    return score


def _map_gdelt_article(article: dict, company_lookup: dict) -> dict:
    """
    GDELT-specific mapping branch.

    GDELT GKG provides server-side NER via TABARI/CAMEO, so spaCy NER is both
    unnecessary AND unable to help (GKG snapshots don't include the article
    body text). Instead we trust GDELT's V1Organizations field, which the
    ingestion DAG already mapped to `symbols_mentioned` at ingestion time.

    Relevance heuristic for GDELT articles:
      base 0.7  if at least one matched ticker
      +0.1     per matched financial theme tag, capped at 1.0
    """
    pre_tagged_tickers = [
        s.replace(".US", "").upper()
        for s in (article.get("symbols_mentioned") or [])
    ]
    pre_tagged_tickers = [t for t in pre_tagged_tickers if t in set(company_lookup.values())]

    n_themes = len(article.get("tags") or [])
    base = 0.7 if pre_tagged_tickers else 0.0
    relevance = min(1.0, base + 0.1 * n_themes)

    relevant = []
    if relevance >= ENTITY_RELEVANCE_THRESHOLD:
        for t in sorted(set(pre_tagged_tickers)):
            relevant.append({
                "ticker": t,
                "relevance": round(relevance, 2),
                "match_type": "gdelt_pretagged",
            })

    return {
        "article_dedup_key": article.get("dedup_key"),
        "relevant_tickers":  relevant,
        "extracted_orgs":    pre_tagged_tickers,
        "clean_content_len": 0,   # GDELT articles have no body text in GKG
    }


def map_article(
    article: dict,
    company_lookup: dict,
    target_ticker: Optional[str] = None,
) -> dict:
    """
    Full entity mapping pipeline for a single article.

    Args:
        article: news article document from MongoDB
        company_lookup: name -> ticker lookup dict
        target_ticker: if set, only score for this ticker

    Returns:
        {
          "article_id": ...,
          "relevant_tickers": [{ticker, relevance, match_type}],
          "extracted_orgs": [...],
          "clean_content_len": int,
        }
    """
    # Branch: GDELT articles bypass spaCy NER (no body text; pre-tagged by GDELT).
    if article.get("source") == "gdelt":
        return _map_gdelt_article(article, company_lookup)

    title = article.get("title", "")
    content = article.get("content", "")

    # Step 1: Strip boilerplate
    clean_content = strip_boilerplate(content)

    # Step 2: NER on title + clean content
    full_text = f"{title}\n\n{clean_content}"
    orgs = extract_orgs(full_text)

    # Step 3: Map to tickers
    ner_matches = map_entities_to_tickers(orgs, company_lookup)

    # Step 4: Score relevance
    relevant_tickers = []

    if target_ticker:
        # Only score for the target ticker
        score = score_relevance(article, target_ticker, company_lookup, ner_matches)
        if score >= ENTITY_RELEVANCE_THRESHOLD:
            match_type = next((m["match_type"] for m in ner_matches if m["ticker"] == target_ticker), "tag_only")
            relevant_tickers.append({
                "ticker": target_ticker,
                "relevance": round(score, 2),
                "match_type": match_type,
            })
    else:
        # Score for all NER-matched tickers + EODHD-tagged tickers
        candidate_tickers = set()
        for m in ner_matches:
            candidate_tickers.add(m["ticker"])
        for sym in (article.get("symbols_mentioned") or []):
            clean_sym = sym.replace(".US", "").upper()
            if clean_sym in [c["ticker"] for c in [{"ticker": t} for t in set(company_lookup.values())]]:
                candidate_tickers.add(clean_sym)

        for ticker in candidate_tickers:
            score = score_relevance(article, ticker, company_lookup, ner_matches)
            if score >= ENTITY_RELEVANCE_THRESHOLD:
                match_type = next((m["match_type"] for m in ner_matches if m["ticker"] == ticker), "tag_only")
                relevant_tickers.append({
                    "ticker": ticker,
                    "relevance": round(score, 2),
                    "match_type": match_type,
                })

    # Sort by relevance descending
    relevant_tickers.sort(key=lambda x: x["relevance"], reverse=True)

    return {
        "article_dedup_key": article.get("dedup_key"),
        "relevant_tickers": relevant_tickers,
        "extracted_orgs": orgs,
        "clean_content_len": len(clean_content),
    }
