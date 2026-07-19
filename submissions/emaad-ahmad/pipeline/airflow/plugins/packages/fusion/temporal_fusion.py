"""
Temporal Fusion Algorithm (RQ2 - Step 2)
=========================================
Implements the sliding-window temporal alignment between price ticks
and news articles.

For each price tick p_t at timestamp t:
    F(t) = {n_i in N : t - Delta <= t_n_i <= t + delta}

where:
    Delta = lookback window  (30 min for intraday, 24h for daily)
    delta = lookahead window (5 min for intraday, 0 for daily)

News matches are ranked by temporal proximity |t_n_i - t|.
Output: fused_events documents stored in MongoDB.

The window parameters are configured per price interval in constants.py
FUSION_WINDOWS dict, making it trivial to switch from daily (demo) to
intraday (production) by changing the interval parameter.

References:
    IPP §3.2, Equation 2: F(t) = {n_i in N : t - Delta <= t_n_i <= t + delta}
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from common.constants import (
    FUSION_WINDOWS,
    US_MARKET_CLOSE_HOUR_UTC,
    ENTITY_RELEVANCE_THRESHOLD,
)

logger = logging.getLogger(__name__)


def price_bar_to_timestamp(price_row: dict) -> datetime:
    """
    Convert a price bar to a reference timestamp for fusion.

    For daily bars:  datetime_utc is just a date (YYYY-MM-DD 00:00),
                     so we set it to US market close (20:00 UTC / 4pm ET)
    For intraday:    datetime_utc already has the exact bar timestamp
    """
    dt = price_row["datetime_utc"]

    if isinstance(dt, str):
        # Parse string -> datetime
        if "T" in dt:
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        else:
            dt = datetime.strptime(dt, "%Y-%m-%d")

    # If naive, assume UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    interval = price_row.get("interval", "1d")

    if interval == "1d":
        # Set to market close time for daily bars
        dt = dt.replace(hour=US_MARKET_CLOSE_HOUR_UTC, minute=0, second=0, microsecond=0)

    return dt


def find_news_in_window(
    price_timestamp: datetime,
    news_articles: list[dict],
    lookback_min: int,
    lookahead_min: int,
) -> list[dict]:
    """
    Find news articles within the sliding window [t - Delta, t + delta].

    Args:
        price_timestamp: reference timestamp of the price tick
        news_articles:   list of news documents with 'published_at' field
        lookback_min:    Delta in minutes
        lookahead_min:   delta in minutes

    Returns:
        list of {article, offset_min} dicts, sorted by |offset|
    """
    window_start = price_timestamp - timedelta(minutes=lookback_min)
    window_end = price_timestamp + timedelta(minutes=lookahead_min)

    matches = []
    for article in news_articles:
        pub_at = article.get("published_at")
        if not pub_at:
            continue

        # Parse published_at
        if isinstance(pub_at, str):
            try:
                pub_dt = datetime.fromisoformat(pub_at.replace("Z", "+00:00"))
            except ValueError:
                continue
        elif isinstance(pub_at, datetime):
            pub_dt = pub_at
        else:
            continue

        if pub_dt.tzinfo is None:
            pub_dt = pub_dt.replace(tzinfo=timezone.utc)

        # Check if within window
        if window_start <= pub_dt <= window_end:
            offset_min = (pub_dt - price_timestamp).total_seconds() / 60.0
            matches.append({
                "article": article,
                "offset_min": round(offset_min, 2),
                "abs_offset_min": abs(offset_min),
            })

    # Sort by temporal proximity (closest first)
    matches.sort(key=lambda x: x["abs_offset_min"])
    return matches


def build_fused_event(
    price_row: dict,
    news_matches: list[dict],
    entity_results: Optional[dict] = None,
    max_news: int = 10,
) -> dict:
    """
    Build a single fused event document from a price tick + matched news.

    Args:
        price_row:      OHLCV price data from PostgreSQL
        news_matches:   list from find_news_in_window()
        entity_results: mapping results from entity_mapper (optional)
        max_news:       cap on news items per fused event

    Returns:
        fused event document ready for MongoDB insertion
    """
    ticker = price_row["ticker"]
    interval = price_row.get("interval", "1d")
    price_ts = price_bar_to_timestamp(price_row)

    # Get the fusion window used
    window_cfg = FUSION_WINDOWS.get(interval, FUSION_WINDOWS["1d"])

    # Build news context array (capped)
    news_context = []
    for match in news_matches[:max_news]:
        art = match["article"]
        ctx = {
            "title": art.get("title", ""),
            "source": art.get("source", ""),
            "published_at": art.get("published_at"),
            "url": art.get("url", ""),
            "offset_min": match["offset_min"],
            "eodhd_sentiment": art.get("eodhd_sentiment"),
        }
        # Add entity relevance if available
        if entity_results:
            dedup_key = art.get("dedup_key")
            if dedup_key and dedup_key in entity_results:
                mapping = entity_results[dedup_key]
                ticker_scores = [t for t in mapping.get("relevant_tickers", []) if t["ticker"] == ticker]
                if ticker_scores:
                    ctx["entity_relevance"] = ticker_scores[0]["relevance"]
                    ctx["match_type"] = ticker_scores[0]["match_type"]

        news_context.append(ctx)

    # Build the fused event
    timestamp_ms = int(price_ts.timestamp() * 1000)

    fused = {
        "ticker": ticker,
        "timestamp_ms": timestamp_ms,
        "datetime_utc": price_ts.isoformat(),
        "interval": interval,
        "price": {
            "open": price_row.get("open"),
            "high": price_row.get("high"),
            "low": price_row.get("low"),
            "close": price_row.get("close"),
            "volume": price_row.get("volume"),
        },
        "fusion_window": {
            "lookback_min": window_cfg["lookback_min"],
            "lookahead_min": window_cfg["lookahead_min"],
        },
        "news_count": len(news_context),
        "news_context": news_context,
        "dedup_key": f"{ticker}|{timestamp_ms}|{interval}",
    }

    return fused


def fuse_ticker(
    ticker: str,
    price_rows: list[dict],
    news_articles: list[dict],
    entity_results: Optional[dict] = None,
) -> list[dict]:
    """
    Run full temporal fusion for a single ticker.

    For each price bar, finds matching news within the sliding window,
    filters by entity relevance, and builds fused events.

    Args:
        ticker:          ticker symbol
        price_rows:      OHLCV rows from PostgreSQL
        news_articles:   articles from MongoDB (already filtered to ticker)
        entity_results:  entity mapping results keyed by dedup_key

    Returns:
        list of fused event documents
    """
    if not price_rows:
        logger.info(f"{ticker}: no price data, skipping fusion")
        return []

    # Determine interval from first row
    interval = price_rows[0].get("interval", "1d")
    window_cfg = FUSION_WINDOWS.get(interval, FUSION_WINDOWS["1d"])

    logger.info(
        f"{ticker}: fusing {len(price_rows)} price bars with {len(news_articles)} articles "
        f"(window: -{window_cfg['lookback_min']}min / +{window_cfg['lookahead_min']}min)"
    )

    # Filter news by entity relevance if available
    if entity_results:
        relevant_articles = []
        for art in news_articles:
            dedup_key = art.get("dedup_key")
            if dedup_key and dedup_key in entity_results:
                mapping = entity_results[dedup_key]
                ticker_scores = [t for t in mapping.get("relevant_tickers", []) if t["ticker"] == ticker]
                if ticker_scores and ticker_scores[0]["relevance"] >= ENTITY_RELEVANCE_THRESHOLD:
                    relevant_articles.append(art)
        logger.info(f"{ticker}: {len(relevant_articles)}/{len(news_articles)} articles passed entity filter")
        news_articles = relevant_articles

    fused_events = []
    for price_row in price_rows:
        price_ts = price_bar_to_timestamp(price_row)

        matches = find_news_in_window(
            price_ts,
            news_articles,
            window_cfg["lookback_min"],
            window_cfg["lookahead_min"],
        )

        fused = build_fused_event(price_row, matches, entity_results)
        fused_events.append(fused)

    events_with_news = sum(1 for e in fused_events if e["news_count"] > 0)
    logger.info(f"{ticker}: created {len(fused_events)} fused events ({events_with_news} have news)")

    return fused_events
