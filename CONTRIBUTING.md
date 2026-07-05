# Contributing Guide — FinStream Pipeline Integration

Thanks for contributing your data pipeline for integration into FinStreamAI. Following this guide closely will make integration much faster for everyone.

## 1. Setup

1. Fork this repository to your own GitHub account.
2. Clone your fork locally.
3. Copy the `template/` folder into `submissions/<your-name>/` (use your GitHub handle or surname, lowercase, no spaces — e.g. `submissions/jsmith/`).
4. Do all your work inside your own `submissions/<your-name>/` folder only. PRs that touch other folders will fail CI automatically.

## 2. Environment compatibility

⚠️ Our production stack is pinned to **Python 3.8, Airflow 2.8.1, PySpark 3.5.1**. Many tutorials, ChatGPT answers, or Stack Overflow posts assume newer library versions and will not run in our environment. Please verify compatibility before you build on top of a new library or API version.

## 3. Development guidelines

- Keep code modular and well-structured.
- No hardcoded local file paths — use config values.
- Separate configuration (paths, API keys, parameters) from business logic. Never commit real credentials — use `config.example.yaml` as a template and keep your real `config.yaml` / `.env` out of git (already covered by `.gitignore`).
- Document in `metadata.yaml`:
  - Data source
  - API endpoint(s)
  - Authentication method
  - Rate limits
  - Output schema
  - Update frequency
- Add sufficient logging so failures can be diagnosed easily.
- Design your pipeline so it can resume gracefully after an interruption, rather than requiring a full re-run.

## 4. Data storage — do not commit data to this repo

**This repository is for code, configuration, and documentation only. Never commit actual data files (raw, intermediate, or final) to git, even in your own submission folder.** GitHub blocks files over 100MB and this repo is not a data store — the `.gitignore` already excludes common data directories, but please don't try to work around it.

Instead, where the data provider's terms permit retaining data:

- Store raw data, intermediate processed data, and the final cleaned dataset in the storage location designated for FinStream (ask Sean/Emmad/Sarah if you're not sure where that is — e.g. shared drive, cloud bucket, database -- recommend to share your google driver link with permision to everyone who has a link).
- Document the storage location and access instructions in `metadata.yaml` under `storage_policy`, so future researchers know where to find the data and can reproduce results or change preprocessing logic without recollecting it from scratch.
- If a small sample (a few KB, e.g. 50–100 rows) would help reviewers sanity-check your pipeline's output shape, you may include that in your submission folder — but this is optional and only for illustration, not as the actual dataset.

## 5. Before opening a pull request

Make sure that:
- Your repo/folder runs from a clean clone.
- All dependencies are listed in `requirements.txt`.
- Your `submissions/<your-name>/README.md` explains how to run the pipeline start to finish.
- No unnecessary files or credentials are included.
- Every item in the PR template checklist is checked off.

## 6. Review process

1. Open a PR from your fork's branch into `main` of this repo.
2. CI will automatically check folder scope, required files, hardcoded paths, and basic secret patterns.
3. Once CI passes and the checklist is complete, request a review from Emmad or Sarah.
4. Approved pipelines will be integrated into FinStreamAI production by the core team.

Questions? Reach out to Sean, Emmad, or Sarah directly.
