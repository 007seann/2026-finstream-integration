# FinStream Pipeline Integration

This repository is the intake point for MSc dissertation data-collection pipelines that will be integrated into **FinStreamAI**, the AIAI lab's real-time heterogeneous financial data infrastructure.

This repo does **not** contain FinStreamAI's production code. It only holds the template, guidelines, and student submissions used for review before integration.

## Quick start (for students)

1. Read [CONTRIBUTING.md](./CONTRIBUTING.md) fully before writing any code.
2. Fork this repo.
3. Copy `template/` to `submissions/<your-name>/`.
4. Build your pipeline there, following the checklist in the PR template.
5. Open a PR back into this repo's `main` branch.

## Structure

```
finstream-pipeline-integration/
├── CONTRIBUTING.md
├── template/
│   ├── pipeline/
│   │   ├── config.example.yaml
│   │   ├── main.py
│   │   └── requirements.txt
│   ├── metadata.yaml
│   └── README.md
└── submissions/
    └── <your-name>/   # created by each student from the template
```

## Review & integration

PRs are validated automatically by CI (folder scope, required files, hardcoded paths, basic secret scanning), then reviewed by Emmad or Sarah. Approved pipelines are integrated into FinStreamAI production by the core team.
