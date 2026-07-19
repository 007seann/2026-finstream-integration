-- =============================================================
-- PostgreSQL Initialization
-- Creates: airflow_metadata DB + financial_data DB with schemas
-- =============================================================

-- Airflow metadata database (Airflow manages its own tables)
CREATE DATABASE airflow_metadata;

-- Financial data tables live in the default financial_data DB
-- (created automatically by POSTGRES_DB env var)

-- =============================================================
-- Table: companies
-- Master list of tracked tickers (S&P 500 focus)
-- =============================================================
CREATE TABLE IF NOT EXISTS companies (
    id              SERIAL PRIMARY KEY,
    ticker          VARCHAR(10) NOT NULL UNIQUE,
    company_name    VARCHAR(255),
    sector          VARCHAR(100),
    industry        VARCHAR(100),
    exchange        VARCHAR(20) DEFAULT 'NYSE',
    cik             VARCHAR(20),           -- SEC Central Index Key

    -- S&P 500 membership tracking (constituents change quarterly):
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    added_at        TIMESTAMP DEFAULT NOW(),
    removed_at      TIMESTAMP NULL,

    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_companies_ticker ON companies(ticker);
CREATE INDEX idx_companies_cik ON companies(cik);
-- Fast active-only lookups for ingestion DAGs
CREATE INDEX idx_companies_active ON companies(is_active) WHERE is_active = TRUE;

-- =============================================================
-- Table: price_data
-- Intraday OHLCV price ticks (1-min / 5-min / 15-min bars)
-- This is the KEY difference from RA platform (they only have 1d)
-- =============================================================
CREATE TABLE IF NOT EXISTS price_data (
    id              BIGSERIAL PRIMARY KEY,
    ticker          VARCHAR(10) NOT NULL,
    timestamp_ms    BIGINT NOT NULL,        -- Unix epoch milliseconds
    datetime_utc    TIMESTAMP NOT NULL,     -- Human-readable UTC timestamp
    open            NUMERIC(12,4),
    high            NUMERIC(12,4),
    low             NUMERIC(12,4),
    close           NUMERIC(12,4),
    volume          BIGINT,
    interval        VARCHAR(5) DEFAULT '1d', -- '1m', '5m', '15m', '1h', '1d'
    source          VARCHAR(20) DEFAULT 'eodhd',
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(ticker, timestamp_ms, interval)
);

-- Primary query pattern: get prices for a ticker within a time range
CREATE INDEX idx_price_ticker_ts ON price_data(ticker, timestamp_ms);
-- For fusion window queries: find prices near a specific timestamp
CREATE INDEX idx_price_ts ON price_data(timestamp_ms);
-- For filtering by interval
CREATE INDEX idx_price_interval ON price_data(interval);

-- =============================================================
-- Table: financial_metrics (future use)
-- Fundamental data per ticker
-- =============================================================
CREATE TABLE IF NOT EXISTS financial_metrics (
    id              SERIAL PRIMARY KEY,
    ticker          VARCHAR(10) NOT NULL REFERENCES companies(ticker),
    report_date     DATE NOT NULL,
    metric_name     VARCHAR(100) NOT NULL,
    metric_value    NUMERIC(20,6),
    source          VARCHAR(50),
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(ticker, report_date, metric_name)
);

-- =============================================================
-- Table: pipeline_runs (SLA tracking for RQ1)
-- =============================================================
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              SERIAL PRIMARY KEY,
    dag_id          VARCHAR(100) NOT NULL,
    run_id          VARCHAR(200),
    start_time      TIMESTAMP NOT NULL,
    end_time        TIMESTAMP,
    records_ingested INTEGER DEFAULT 0,
    latency_seconds NUMERIC(10,2),         -- End-to-end latency
    status          VARCHAR(20) DEFAULT 'running', -- running, success, failed
    error_message   TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_pipeline_dag ON pipeline_runs(dag_id, start_time);

-- Seed a few demo tickers (top S&P 500 by market cap)
INSERT INTO companies (ticker, company_name, sector, cik) VALUES
    ('AAPL', 'Apple Inc.', 'Technology', '0000320193'),
    ('MSFT', 'Microsoft Corporation', 'Technology', '0000789019'),
    ('GOOGL', 'Alphabet Inc.', 'Technology', '0001652044'),
    ('AMZN', 'Amazon.com Inc.', 'Consumer Discretionary', '0001018724'),
    ('NVDA', 'NVIDIA Corporation', 'Technology', '0001045810'),
    ('TSLA', 'Tesla Inc.', 'Consumer Discretionary', '0001318605'),
    ('META', 'Meta Platforms Inc.', 'Technology', '0001326801'),
    ('JPM', 'JPMorgan Chase & Co.', 'Financials', '0000019617'),
    ('V', 'Visa Inc.', 'Financials', '0001403161'),
    ('JNJ', 'Johnson & Johnson', 'Health Care', '0000200406')
ON CONFLICT (ticker) DO NOTHING;
