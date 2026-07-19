# Samples

Small illustrative shape-check documents (each ~1–2 KB). These are **for
reviewer sanity-check only** — the real corpus lives in Docker volumes on
the dev machine (503 tickers, 3.21M price rows, 3.21M fused events).

| File | Represents |
|---|---|
| `sample_price_row.json`         | One row from PostgreSQL `price_data` (5m interval). |
| `sample_news_article_eodhd.json`| One document from MongoDB `news_articles` (`source=eodhd`). Content body elided per EODHD ToS. |
| `sample_news_article_gdelt.json`| One document from MongoDB `news_articles` (`source=gdelt`). GDELT is metadata-only — no body text ever exists. |
| `sample_fused_event.json`       | One fused_events document, showing the full `news_context[]` shape with FinBERT/RoBERTa/EODHD/GDELT sentiment attached. |
| `sample_sentiment_score.json`   | One document from MongoDB `sentiment_scores` (one (article, model) pair). |

None of these files contain any real EODHD-copyrighted content bodies —
titles and content strings in EODHD-sourced samples are elided placeholders.
All 5 files are valid UTF-8 JSON and can be loaded with `json.loads()`.
