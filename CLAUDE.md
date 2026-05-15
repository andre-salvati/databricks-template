# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A production-ready PySpark/Databricks ETL pipeline template using medallion architecture, Python packaging, unit + integration tests, Databricks Declarative Automation Bundles (DABs), and DQX data quality framework. Code is structured as a Python wheel package (not notebooks) deployed to Databricks serverless.

## Tooling: Databricks AI Dev Kit + MCP

This project is developed with the [Databricks AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit) installed at the user level (`~/.ai-dev-kit/`). It provides:

- **Databricks MCP server** (`mcp__databricks__*` tools) — wired globally in `~/.claude.json`, authenticates via the `DEFAULT` profile in `~/.databrickscfg`.
- **Databricks skills** (`databricks-bundles`, `databricks-jobs`, `databricks-python-sdk`, `databricks-config`, `databricks-unity-catalog`, etc.) — invoke via the Skill tool when the task matches.

### When to use what

- **Workspace/UC/Jobs/Pipelines/Apps/Serving operations** → prefer `mcp__databricks__*` tools over `databricks` CLI shell-outs or hand-rolled SDK scripts. Examples: `manage_jobs`, `manage_job_runs`, `manage_uc_objects`, `execute_sql`, `manage_serving_endpoint`, `manage_workspace_files`.
- **Bundle work** (editing `databricks.yml`, `resources/*.yml`, deploy/run) → invoke the `databricks-bundles` skill. Note this project generates `resources/jobs.yml` via `scripts/sdk_generate_template_job.py`; do not hand-edit it.
- **Adding/modifying jobs** → invoke `databricks-jobs` skill for guidance, but route changes through `scripts/sdk_generate_template_job.py` + `make deploy` (see "Adding a New Job" below).
- **Switching workspaces / checking auth** → invoke `databricks-config` skill.
- **SDK code inside `src/template/`** → invoke `databricks-python-sdk` skill.

### Conventions

- Use the `DEFAULT` profile unless told otherwise. To check or switch, use the `databricks-config` skill.
- Do **not** install the Dev Kit into this repo or commit MCP config — it's a user-level tool. `.claude/` is currently untracked.
- If MCP tools are unavailable in a session, fall back to the `databricks` CLI or `databricks-sdk` directly, but flag it to the user.

## Commands

```bash
make sync              # Install all dependencies via uv
make test              # Run pytest with coverage
make pre-commit        # Update and run pre-commit hooks (ruff lint/format)
make create-sp         # Create service principal for staging/prod
make deploy env=dev    # Generate jobs.yml + deploy bundle to target env (dev/staging/prod)
make run env=staging   # Run integration test job on staging
```

Run a single test file:
```bash
uv run pytest tests/job1/unit_test.py
```

Run a single test by name:
```bash
uv run pytest tests/job1/unit_test.py::test_enrich_orders
```

## Architecture

### Execution Flow

`main.py` parses CLI args → instantiates `Config` → dispatches to a task class via `TASKS` dict → calls `.run()`.

Each Databricks job task maps to one class. The `--task` arg value must match a key in `TASKS`. Job definitions are **generated** (not hand-authored) by `scripts/sdk_generate_template_job.py` into `resources/jobs.yml`, which is then consumed by the bundle. Never edit `resources/jobs.yml` directly.

### Key Classes

- **`Config`** ([src/template/config.py](src/template/config.py)) — holds all runtime params, creates the `SparkSession`, sets the Unity Catalog default, and exposes `dq_engine` (DQX). When `env=local` (unit tests), it mocks the `WorkspaceClient` so tests run without Databricks connectivity.
- **`BaseTask`** ([src/template/baseTask.py](src/template/baseTask.py)) — minimal base class; gives every task `self.spark` and `self.config`.
- **Task classes** (e.g. `ExtractSource1`, `GenerateOrders`) — subclass `BaseTask`, implement `run()`. Transformation logic lives in dedicated methods (e.g. `enrich_order`) so unit tests can call them directly without Spark tables.

### Medallion Layers (Unity Catalog Schemas)

| Schema | Content |
|---|---|
| `external_source` | Raw input data (seeded by integration test `setup` task) |
| `raw` | Bronze — direct copies from sources |
| `curated` | Silver — joined/enriched tables |
| `report` | Gold — aggregated tables |

### Environments & Catalogs

- `dev` — catalog name = developer's short username (isolated per developer, prevents concurrency conflicts)
- `staging` / `prod` — catalog name matches environment; runs as service principal

### Data Quality (DQX)

`ExtractSource2` demonstrates the DQX pattern: define rules as `DQRowRule`/`DQForEachColRule`/`DQDatasetRule`, call `dq_engine.apply_checks_and_split()`, write invalid rows to a `_quarantine` table.

### Testing Pattern

Unit tests use `env=local` which bypasses Databricks catalog setup and mocks `WorkspaceClient`. Test transformation methods directly (e.g., `task.enrich_order(df1, df2, df3)`) using in-memory DataFrames. Integration tests use the `setup` → `run` → `validate` job sequence on staging.

### CI/CD (GitHub Actions)

On every push: install deps → unit tests → deploy to staging → run integration tests → deploy to prod. Requires `DATABRICKS_HOST` and `DATABRICKS_TOKEN` secrets.

### Adding a New Job

1. Create task classes under `src/template/<jobN>/`, inheriting `BaseTask`.
2. Register them in `TASKS` dict in `main.py`.
3. Add task construction logic to `scripts/sdk_generate_template_job.py`.
4. Run `make deploy env=dev` to regenerate `resources/jobs.yml` and deploy.
