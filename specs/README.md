# Specs

Project specifications for `databricks-template`. These hold the deep technical detail that used to
live in the root `README.md`; the README is now a landing page (overview, features, dashboard,
setup), `CLAUDE.md` holds the every-session working rules, and these specs are the canonical
reference. Read the relevant spec before working in that area.

| Spec | Read it when you're touching… |
|---|---|
| [architecture.md](architecture.md) | the wheel/CLI surface, jobs DAG, job generation, CI/CD, job-level params, deploy-time env vars, logging, or production guardrails. |
| [data-model.md](data-model.md) | the catalog/schema model, medallion flow, table schemas, the product-name freeze semantics, liquid clustering, DQX/quarantine, or lineage. |
| [test-plan.md](test-plan.md) | unit, integration, or load tests. |
| [tooling.md](tooling.md) | MCP servers (Databricks, AWS, context7), the Databricks CLI, and the bundled skills — what to reach for and when. |
| [CHANGELOG.md](CHANGELOG.md) | the per-PR change history. **Append-only — add an entry before every merge; don't read it for context.** |

## Folder structure

```
databricks-template/
├── .github/workflows/onpush.yml   # CI/CD pipeline
├── src/template/                  # Python package (deployed as a wheel)
│   ├── main.py                    # CLI entry point + TASKS dict
│   ├── config.py                  # Config: catalogs/schemas, logging, DQX
│   ├── baseTask.py                # BaseTask (spark/config/logger/cluster_by)
│   ├── commonSchemas.py           # Canonical PySpark schemas
│   └── job1/                      # extract_source1/2, generate_orders(_agg),
│                                  #   health_check, seed_sources
├── tests/job1/                    # unit_test, unit_test_sdp, integration_setup/validate
├── resources/                     # jobs.yml (generated), orders_dashboard.lvdash.json (committed)
├── scripts/                       # sdk_generate_template_job.py, sdk_init_workspace.py,
│                                  #   sdk_drop_tables.py, project_costs.py, _sdk_sql.py
├── specs/                         # architecture / data-model / test-plan / tooling (this folder)
├── assets/                        # screenshots + diagrams referenced by README + specs
├── databricks.yml · pyproject.toml · Makefile · .pre-commit-config.yaml
```

## Diagrams

The medallion diagram is inline Mermaid (rendered by GitHub) — edit the fenced block. The CI/CD
diagram is a draw.io export (`assets/ci_cd.drawio` → `assets/ci_cd.png`, edit in
https://app.diagrams.net); the other PNGs in `../assets/` are real UI screenshots.
