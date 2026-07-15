-- =============================================================
-- Migration: add S&P 500 membership tracking to `companies` table
-- Date:      2026-05-28
-- Purpose:   Lets the platform track quarterly S&P 500 constituent
--            changes without losing historical data for delisted
--            tickers. Idempotent — safe to re-run.
-- =============================================================

-- Add columns (no-op if already present)
ALTER TABLE companies
    ADD COLUMN IF NOT EXISTS is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS added_at    TIMESTAMP DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS removed_at  TIMESTAMP NULL;

-- Partial index for fast "active ticker" lookups by ingestion DAGs
CREATE INDEX IF NOT EXISTS idx_companies_active
    ON companies(is_active)
    WHERE is_active = TRUE;

-- Verify
SELECT
    COUNT(*)                                    AS total_companies,
    COUNT(*) FILTER (WHERE is_active = TRUE)    AS active_companies,
    COUNT(*) FILTER (WHERE is_active = FALSE)   AS inactive_companies
FROM companies;
