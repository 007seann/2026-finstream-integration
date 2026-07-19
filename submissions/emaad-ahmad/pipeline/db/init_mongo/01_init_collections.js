// =============================================================
// MongoDB Initialization
// Database: financial_db
// Collections: news_articles, sec_filings, transcripts, fused_events
// =============================================================

// Switch to the financial database
db = db.getSiblingDB('financial_db');

// Authenticate (running inside Docker init, credentials from env)
// Docker handles auth via MONGO_INITDB_ROOT_USERNAME/PASSWORD

// =============================================================
// Collection: news_articles
// Source: EODHD Financial News API
// =============================================================
db.createCollection('news_articles');
db.news_articles.createIndex(
    { "ticker": 1, "published_at": -1 },
    { name: "idx_ticker_date" }
);
db.news_articles.createIndex(
    { "dedup_key": 1 },
    { name: "idx_dedup", unique: true }
);
db.news_articles.createIndex(
    { "published_at": -1 },
    { name: "idx_published" }
);
db.news_articles.createIndex(
    { "source": 1, "ticker": 1 },
    { name: "idx_source_ticker" }
);

// =============================================================
// Collection: sec_filings
// Source: SEC EDGAR API (10-K, 10-Q)
// =============================================================
db.createCollection('sec_filings');
db.sec_filings.createIndex(
    { "cik": 1, "filing_date": -1 },
    { name: "idx_cik_date" }
);
db.sec_filings.createIndex(
    { "ticker": 1, "form_type": 1 },
    { name: "idx_ticker_form" }
);
db.sec_filings.createIndex(
    { "dedup_key": 1 },
    { name: "idx_dedup", unique: true }
);

// =============================================================
// Collection: transcripts
// Source: API Ninjas (earnings call transcripts)
// =============================================================
db.createCollection('transcripts');
db.transcripts.createIndex(
    { "ticker": 1, "year": -1, "quarter": -1 },
    { name: "idx_ticker_period" }
);
db.transcripts.createIndex(
    { "dedup_key": 1 },
    { name: "idx_dedup", unique: true }
);

// =============================================================
// Collection: fused_events
// Output of temporal fusion algorithm (RQ2)
// Each document = one price tick + attached news context
// =============================================================
db.createCollection('fused_events');
db.fused_events.createIndex(
    { "ticker": 1, "timestamp_ms": -1 },
    { name: "idx_ticker_ts" }
);
db.fused_events.createIndex(
    { "timestamp_ms": -1 },
    { name: "idx_ts" }
);

// =============================================================
// Collection: sentiment_scores
// Output of FinBERT/RoBERTa inference (RQ3)
// =============================================================
db.createCollection('sentiment_scores');
db.sentiment_scores.createIndex(
    { "ticker": 1, "article_id": 1 },
    { name: "idx_ticker_article" }
);
db.sentiment_scores.createIndex(
    { "model": 1, "scored_at": -1 },
    { name: "idx_model_date" }
);

print("=== MongoDB initialized: financial_db with 5 collections ===");
print("  - news_articles (EODHD news)");
print("  - sec_filings (SEC EDGAR 10-K/10-Q)");
print("  - transcripts (earnings calls)");
print("  - fused_events (temporal fusion output)");
print("  - sentiment_scores (FinBERT/RoBERTa output)");
