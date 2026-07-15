# =============================================================================
# Platform Data Quality Audit
# Queries every layer (L1 price, L1 news, L2 fusion, L3 sentiment, DAG runs)
# for both correctness and coverage. Run at any time.
#
# Usage:  cd project root, then:  .\scripts\audit_all_layers.ps1
# =============================================================================

$ErrorActionPreference = "Continue"

function Write-Section($title) {
    Write-Host ""
    Write-Host ("=" * 70) -ForegroundColor Cyan
    Write-Host "  $title" -ForegroundColor Cyan
    Write-Host ("=" * 70) -ForegroundColor Cyan
}

function Run-Mongo($jsInline) {
    # Helper: write JS to temp file, exec inside container (avoids PS quote hell)
    $tmp = New-TemporaryFile
    Set-Content -Path $tmp.FullName -Value $jsInline
    docker cp $tmp.FullName finplatform_mongodb:/tmp/audit.js | Out-Null
    docker exec finplatform_mongodb mongosh -u finplatform -p finplatform_dev_2026 --authenticationDatabase admin financial_db --quiet -f /tmp/audit.js
    Remove-Item $tmp.FullName -ErrorAction SilentlyContinue
}

function Run-Postgres($sql) {
    docker exec finplatform_postgres psql -U finplatform -d financial_data -c "$sql"
}

# --------------------------------------------------------------------------
Write-Section "0.  Overall Platform Snapshot (FastAPI /v1/stats + quota)"
# --------------------------------------------------------------------------
curl "http://localhost:8000/v1/stats" 2>&1 | Select-Object -Property Content
Write-Host ""
curl "http://localhost:8000/v1/eodhd/usage" 2>&1 | Select-Object -Property Content

# --------------------------------------------------------------------------
Write-Section "1.  L1 PRICE (PostgreSQL, structured OHLCV)"
# --------------------------------------------------------------------------

Write-Host "`n[1.1]  Row counts by interval:"
Run-Postgres "SELECT interval, COUNT(*) AS rows, COUNT(DISTINCT ticker) AS tickers, MIN(datetime_utc) AS earliest, MAX(datetime_utc) AS latest FROM price_data GROUP BY interval ORDER BY interval;"

Write-Host "`n[1.2]  Top-10 tickers by row count (should be diverse, not skewed to a few):"
Run-Postgres "SELECT ticker, COUNT(*) AS rows FROM price_data GROUP BY ticker ORDER BY rows DESC LIMIT 10;"

Write-Host "`n[1.3]  Tickers in companies table with ZERO price rows (missing coverage):"
Run-Postgres "SELECT COUNT(*) AS missing_price_tickers FROM companies c WHERE c.is_active AND NOT EXISTS (SELECT 1 FROM price_data p WHERE p.ticker = c.ticker);"

Write-Host "`n[1.4]  Data quality: rows with any NULL OHLCV field:"
Run-Postgres "SELECT COUNT(*) AS null_ohlcv_rows FROM price_data WHERE open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL OR volume IS NULL;"

Write-Host "`n[1.5]  Consistency: rows where high < low (should be zero):"
Run-Postgres "SELECT COUNT(*) AS invalid_high_low FROM price_data WHERE high < low;"

# --------------------------------------------------------------------------
Write-Section "2.  L1 NEWS (MongoDB, news_articles)"
# --------------------------------------------------------------------------

Write-Host "`n[2.1]  By source (with earliest / latest):"
Run-Mongo 'db.news_articles.aggregate([{$group:{_id:"$source",count:{$sum:1},earliest:{$min:"$published_at"},latest:{$max:"$published_at"}}}]).forEach(printjson)'

Write-Host "`n[2.2]  Top-10 tickers by news volume:"
Run-Mongo 'db.news_articles.aggregate([{$group:{_id:"$ticker",count:{$sum:1}}},{$sort:{count:-1}},{$limit:10}]).forEach(printjson)'

Write-Host "`n[2.3]  Distinct tickers covered by news:"
Run-Mongo 'print("distinct_tickers_in_news: " + db.news_articles.distinct("ticker").length)'

Write-Host "`n[2.4]  EODHD data-quality: articles with empty title (should be 0 for source=eodhd):"
Run-Mongo 'print("eodhd_empty_title: " + db.news_articles.countDocuments({source:"eodhd", title:""}))'

Write-Host "`n[2.5]  EODHD sentiment coverage: articles WITH pre-scored eodhd_sentiment:"
Run-Mongo 'print("eodhd_with_sentiment: " + db.news_articles.countDocuments({source:"eodhd", eodhd_sentiment:{$ne:null}}))'

Write-Host "`n[2.6]  GDELT tone coverage: articles WITH gdelt_tone:"
Run-Mongo 'print("gdelt_with_tone: " + db.news_articles.countDocuments({source:"gdelt", gdelt_tone:{$exists:true}}))'

# --------------------------------------------------------------------------
Write-Section "3.  L2 FUSION (MongoDB, fused_events)"
# --------------------------------------------------------------------------

Write-Host "`n[3.1]  Fused events summary:"
Run-Mongo 'db.fused_events.aggregate([{$group:{_id:"$interval",count:{$sum:1},avg_news:{$avg:"$news_count"},max_news:{$max:"$news_count"}}}]).forEach(printjson)'

Write-Host "`n[3.2]  Fused events by news attachment tier:"
Run-Mongo 'db.fused_events.aggregate([{$bucket:{groupBy:"$news_count",boundaries:[0,1,3,10,999],default:"other",output:{count:{$sum:1}}}}]).forEach(printjson)'

Write-Host "`n[3.3]  Fused events with GDELT news attached (RQ2 milestone):"
Run-Mongo 'print("gdelt_in_fused: " + db.fused_events.countDocuments({"news_context.source":"gdelt"}))'

Write-Host "`n[3.4]  Fused events with EODHD news attached:"
Run-Mongo 'print("eodhd_in_fused: " + db.fused_events.countDocuments({"news_context.source":"eodhd"}))'

Write-Host "`n[3.5]  Ticker diversity in GDELT-attached fused events:"
Run-Mongo 'db.fused_events.aggregate([{$match:{"news_context.source":"gdelt"}},{$group:{_id:"$ticker",fused:{$sum:1}}},{$sort:{fused:-1}}]).forEach(printjson)'

# --------------------------------------------------------------------------
Write-Section "4.  L3 SENTIMENT (MongoDB, sentiment_scores)"
# --------------------------------------------------------------------------

Write-Host "`n[4.1]  Total sentiment scores by model:"
Run-Mongo 'db.sentiment_scores.aggregate([{$group:{_id:"$model",count:{$sum:1},avg_conf:{$avg:"$confidence"}}}]).forEach(printjson)'

Write-Host "`n[4.2]  Label distribution per model:"
Run-Mongo 'db.sentiment_scores.aggregate([{$group:{_id:{model:"$model",label:"$label"},count:{$sum:1}}},{$sort:{"_id.model":1,"_id.label":1}}]).forEach(printjson)'

Write-Host "`n[4.3]  3-way agreement per model:"
Run-Mongo 'db.sentiment_scores.aggregate([{$match:{eodhd_comparison:{$exists:true}}},{$group:{_id:"$model",total:{$sum:1},agree:{$sum:{$cond:["$eodhd_comparison.agreement",1,0]}}}}]).forEach(printjson)'

Write-Host "`n[4.4]  Confidence distribution buckets:"
Run-Mongo 'db.sentiment_scores.aggregate([{$bucket:{groupBy:"$confidence",boundaries:[0,0.5,0.75,0.9,1.0],default:"other",output:{count:{$sum:1}}}}]).forEach(printjson)'

Write-Host "`n[4.5]  Scored_at date range:"
Run-Mongo 'const r = db.sentiment_scores.aggregate([{$group:{_id:null,earliest:{$min:"$scored_at"},latest:{$max:"$scored_at"}}}]).toArray()[0]; print("earliest: " + r.earliest); print("latest:   " + r.latest)'

# --------------------------------------------------------------------------
Write-Section "5.  AIRFLOW DAG RUNS (recent status)"
# --------------------------------------------------------------------------

$dags = @(
    "sp500_refresh_pipeline",
    "eodhd_price_pipeline",
    "eodhd_news_pipeline",
    "gdelt_news_pipeline",
    "temporal_fusion_pipeline",
    "sentiment_enrichment_pipeline"
)
foreach ($dag in $dags) {
    Write-Host "`n[5.$($dags.IndexOf($dag)+1)]  Last 3 runs of $dag :"
    docker exec finplatform_airflow airflow dags list-runs -d $dag 2>&1 | Select-Object -First 5
}

Write-Section "AUDIT COMPLETE"
