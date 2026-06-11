# Specs

Project specifications for `databricks-template`. These hold the deep technical detail that used to
live in the root `README.md`; the README is now a landing page (overview, features, dashboard,
setup), `CLAUDE.md` holds the working rules, and these specs are the canonical reference. Read the
relevant spec before working in that area.

| Spec | Read it when you're touching… |
|---|---|
| [architecture.md](architecture.md) | the wheel/CLI surface, jobs DAG, job generation, CI/CD, job-level params, deploy-time env vars, logging, or production guardrails. |
| [data-model.md](data-model.md) | the catalog/schema model, medallion flow, table schemas, the price-freeze semantics, liquid clustering, DQX/quarantine, or lineage. |
| [test-plan.md](test-plan.md) | unit, integration, or load tests. |
| [CHANGELOG.md](CHANGELOG.md) | the per-PR change history (add an entry before every merge). |

The medallion diagram is inline Mermaid (rendered by GitHub) — edit the fenced block. The CI/CD
diagram is a draw.io export (`docs/ci_cd.drawio` → `docs/ci_cd.png`, edit in https://app.diagrams.net);
the other PNGs in `../docs/` are real UI screenshots.
