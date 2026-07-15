"""
Constants for the financial data platform.
Collection names, API endpoints, and default configurations.
"""

# =============================================================
# MongoDB Collections
# =============================================================
NEWS_COLLECTION = "news_articles"
SEC_COLLECTION = "sec_filings"
TRANSCRIPTS_COLLECTION = "transcripts"
FUSED_EVENTS_COLLECTION = "fused_events"
SENTIMENT_COLLECTION = "sentiment_scores"

# =============================================================
# API Endpoints
# =============================================================
EODHD_NEWS_URL = "https://eodhd.com/api/news"
EODHD_EOD_URL = "https://eodhd.com/api/eod"
EODHD_INTRADAY_URL = "https://eodhd.com/api/intraday"

# [DISABLED 2026-05-28] SEC EDGAR ingestion out of scope. URLs retained for reference.
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
SEC_FILING_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}"

NINJA_TRANSCRIPT_URL = "https://api.api-ninjas.com/v1/earningstranscript"

# =============================================================
# GDELT GKG 2.0 (free, no API key)
# Publishes a global news knowledge-graph snapshot every 15 minutes.
# Docs: https://www.gdeltproject.org/data.html
# =============================================================
GDELT_LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
GDELT_HTTP_TIMEOUT_S = 60
GDELT_GKG_FIELD_COUNT = 27   # GKG 2.0 row width

# Financial-relevant GDELT theme tags. An article is kept only if at least
# one of these themes is present in its V1Themes column.
# Reference: https://www.gdeltproject.org/data.html#themes
GDELT_FINANCIAL_THEMES = {
    "ECON_STOCKMARKET",
    "ECON_EARNINGSREPORT",
    "ECON_BANKRUPTCY",
    "ECON_INTEREST_RATES",
    "ECON_MONETARY_POLICY",
    "ECON_INFLATION",
    "ECON_TRADE",
    "BUS_STOCK_BUYBACK",
    "BUS_MARKET_CLOSE",
    "BUS_NEW_PRODUCTS",
    "MERGER",
    "ACQUISITION",
    "LEG_FINANCIAL_REGULATION",
}

# Company-name suffixes to strip when normalising org names for ticker lookup.
ORG_NAME_SUFFIXES = (
    " inc.", " inc", " corp.", " corp", " corporation",
    " company", " co.", " co", " ltd.", " ltd", " plc",
    " limited", " group", " holdings", " holding",
)

# =============================================================
# Default Tickers (demo set - top 10 S&P 500 by market cap)
# =============================================================
DEMO_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    "TSLA", "META", "JPM", "V", "JNJ",
]

# =============================================================
# Fusion Parameters (from IPP: Equation 2)
# Configurable per interval — swap when upgrading to intraday
# =============================================================
FUSION_LOOKBACK_MINUTES = 30   # Delta = 30 min (intraday default)
FUSION_LOOKAHEAD_MINUTES = 5   # delta = 5 min  (intraday default)

# Per-interval fusion windows (minutes)
# Daily bars: wide window (24h lookback from market close)
# Intraday: narrow window per IPP spec
FUSION_WINDOWS = {
    "1d":  {"lookback_min": 1440, "lookahead_min": 5760},  # 24h back, 4 days ahead (bridges weekends/holidays)
    "1h":  {"lookback_min": 120,  "lookahead_min": 10},    # 2 hours
    "15m": {"lookback_min": 60,   "lookahead_min": 5},     # 1 hour
    "5m":  {"lookback_min": 30,   "lookahead_min": 5},     # IPP default
    "1m":  {"lookback_min": 15,   "lookahead_min": 2},     # Tight
}

# US market hours (for converting daily bars to timestamps)
US_MARKET_CLOSE_HOUR_UTC = 20  # 4:00 PM ET = 20:00 UTC (EST+5)

# =============================================================
# Entity Mapping (RQ2)
# =============================================================
# Boilerplate patterns to strip before NER
BOILERPLATE_PATTERNS = [
    "Free Stock Analysis Report",
    "originally appeared on Benzinga",
    "originally appeared on",
    "View Comments",
    "Story Continues",
    "Don't Miss:",
    "Read Next:",
    "Trending:",
    "See Also:",
    "Top Picks",
    "UNLOCKED: 5 NEW TRADES",
    "Get the latest stock analysis",
    "was originally published by The Motley Fool",
    "Stock Advisor returns as of",
    "The Motley Fool has positions in",
    "This article provides information only",
]

# Minimum relevance score to keep a ticker-article association
ENTITY_RELEVANCE_THRESHOLD = 0.3

# =============================================================
# Sentiment Models (RQ3)
# =============================================================
FINBERT_MODEL = "ProsusAI/finbert"
ROBERTA_MODEL = "mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis"
MINILM_MODEL = "all-MiniLM-L6-v2"      # Fallback for SLA breach

SENTIMENT_SLA_SECONDS = 300  # 5-minute max inference time per batch
SENTIMENT_BATCH_SIZE = 16    # Articles per inference batch

# =============================================================
# Rate Limits
# =============================================================
SEC_RATE_LIMIT = 10            # [DISABLED] 10 requests per second (SEC policy)
EODHD_FREE_DAILY_LIMIT = 20   # Free tier: 20 API calls per day
