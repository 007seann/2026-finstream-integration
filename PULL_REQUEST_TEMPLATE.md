## Student / Dissertation Project
- Name:
- Data source(s) covered by this pipeline:

## Submission Checklist

Please do not request review until every box is checked. Reviews for incomplete PRs will be closed and re-requested later.

**Documentation**
- [ ] `metadata.yaml` completed (data source, API endpoint(s), auth method, rate limits, output schema, update frequency)
- [ ] README in your submission folder explains how to run the pipeline end-to-end from a clean clone

**Code quality**
- [ ] No hardcoded local file paths (e.g. `/Users/...`, `/home/...`, `C:\...`)
- [ ] Configuration values (API keys, paths, params) are separated from business logic
- [ ] Sufficient logging included so failures can be diagnosed
- [ ] Pipeline can resume gracefully after an interruption (no full re-run required)
- [ ] Library/dependency versions verified compatible with our pinned stack (Python 3.8, Airflow 2.8.1, PySpark 3.5.1) — newer-API tutorials/ChatGPT snippets often break here

**Data handling**
- [ ] Raw data retention policy stated (where provider terms allow it)
- [ ] Intermediate processed data retention policy stated
- [ ] Final cleaned dataset format/location documented

**Repo hygiene**
- [ ] Runs from a clean clone (no missing files, no local-only dependencies)
- [ ] `requirements.txt` present and complete
- [ ] No credentials, tokens, or `.env` files committed
- [ ] Changes are contained to your own `submissions/<your-name>/` folder only

## Notes for reviewers
Anything you want Emmad/Sarah/Sean to pay special attention to:
