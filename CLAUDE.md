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
make init              # One-time workspace bootstrap (SP, catalogs, schemas, grants). Edit S3 path first.
                       # If workspace has >1 SQL warehouse, pass --warehouse-name to the underlying script.
make deploy env=dev    # Generate jobs.yml + deploy bundle to target env (dev/staging/prod)
make run env=dev       # Run integration test job on a target env (dev or staging)
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

### CLI surface

The wheel entry point is intentionally minimal:

- `--task` *(required)* — task key. In jobs, `{{task.name}}` fills this.
- `--env` *(required)* — `dev` / `staging` / `prod` (or `local` for tests).
- `--run-id` *(optional, observability-only)* — filled by Databricks via `{{job.run_id}}`. Stamped onto every log line via a `logging.Filter` so logs are correlatable after ingest. Defaults to `-` when absent (e.g. local tests).
- `--log-level` *(optional)* — `DEBUG`/`INFO`/`WARNING`. Filled from the job-level parameter `log_level` (default `INFO`). Override per-run from the Databricks Jobs UI "Run with different parameters" dialog.
- `--quarantine-fail-ratio` *(optional)* — float threshold for DQX hard-fail in `extract_source2`. Filled from the job-level parameter `quarantine_fail_ratio` (default `1.0` in dev/staging, `0.1` in prod).
- `--seed-date` *(optional)* — ISO-8601 date (e.g. `2024-03-15`) consumed by `seed_sources`. Filled from the job-level parameter `seed_date` (default `""` → resolved to today at runtime). Override per-run to backfill a specific day.

Anything tunable at runtime is a **CLI arg** populated from a Databricks job-level parameter — not an environment variable. Serverless compute does not expose custom env vars to the process.

### Key Classes

- **`Config`** ([src/template/config.py](src/template/config.py)) — runtime config: catalog/schema setup, logging, DQX engine. When `env=local` (unit tests), it mocks the `WorkspaceClient` so tests run without Databricks connectivity.
- **`BaseTask`** ([src/template/baseTask.py](src/template/baseTask.py)) — base class giving every task `self.spark`, `self.config`, and `self.logger`.
- **Task classes** (e.g. `ExtractSource1`, `GenerateOrders`, `HealthCheck`) — subclass `BaseTask`, implement `run()`. Transformation logic lives in dedicated methods (e.g. `enrich_order`) so unit tests can call them directly without Spark tables.

### Catalog / schema model (load-bearing)

**Environment isolation is at the *catalog* level, not the schema level.** Same medallion schemas exist in every catalog.

- `dev_{sanitized_user}` — per-developer sandbox; created lazily by `Config.__init__`. Username is `WorkspaceClient().current_user.me().user_name.split("@")[0]` with non-alphanumerics replaced by `_` (e.g. `andre.f.salvati` → `andre_f_salvati`).
- `staging`, `prod` — shared; provisioned upfront by `make init` (`scripts/sdk_init_workspace.py`), which creates the catalogs, all `MEDALLION_SCHEMAS`, and the required grants. Runtime jobs in these envs must NOT have `CREATE CATALOG` or `CREATE SCHEMA` privilege — those operations belong to the bootstrap script, not the runtime wheel.

Medallion schemas (`MEDALLION_SCHEMAS` in `config.py`):

| Schema | Content |
|---|---|
| `external_source` | Raw input data — populated by `seed_sources` task (prod only, daily); seeded with controlled data by the integration test `setup` task (dev/staging) |
| `raw` | Bronze — direct copies from sources |
| `curated` | Silver — joined/enriched tables |
| `report` | Gold — aggregated tables |
| `ops` | Internal — health-check table. Named `ops` instead of `system` because Unity Catalog reserves `system`. |

Each task's input/output tables are **hardcoded** in the task module (e.g. `raw.customer` → `curated.order_enriched`). The medallion layer is a semantic contract, not a runtime parameter — this is the dbt `ref()` pattern. Don't parameterize the layer; if a task genuinely needs a configurable target, that's a different task.

### Job-level parameters (runtime, overridable per-run)

Defined as `JobParameterDefinition` in `sdk_generate_template_job.py` and referenced in every task's `parameters` list via `{{job.parameters.*}}`. Operators can override them per-run from the Databricks Jobs UI "Run with different parameters" dialog by name — no need to rewrite the entire task parameters array.

| Parameter | Purpose | Default (dev/staging) | Default (prod) |
|---|---|---|---|
| `log_level` | `DEBUG`/`INFO`/`WARNING`. Bump to `DEBUG` for a single run during prod incident response. | `INFO` | `INFO` |
| `quarantine_fail_ratio` | Hard-fail `extract_source2` if more than this fraction of rows are quarantined by DQX. | `1.0` (disabled) | `0.1` |
| `seed_date` | ISO-8601 date consumed by `seed_sources`. Empty string (default) resolves to today at runtime. Override to backfill a specific day (e.g. `"2024-03-15"`). | `""` → today | `""` → today |

### Deploy-time environment variables (CI/build machine only)

These are read by `sdk_generate_template_job.py` at deploy time — never on Databricks serverless. Use `os.environ.get()` is correct here.

| Variable | Purpose | Default |
|---|---|---|
| `TEMPLATE_ALERT_EMAILS` | Comma-separated recipients for prod `JobEmailNotifications`. CI overrides via secret. | `data-platform-oncall@example.com` |
| `TEMPLATE_SP_APP_ID` | CI bypass for the SCIM lookup of the service principal. | resolved from `SP_DISPLAY_NAME` |

### Data Quality (DQX)

`ExtractSource2` demonstrates the DQX pattern: define rules as `DQRowRule`/`DQForEachColRule`/`DQDatasetRule`, call `dq_engine.apply_checks_and_split()`, write invalid rows to a `_quarantine` table. The `--quarantine-fail-ratio` job parameter hard-fails the task when too many rows are quarantined (silent quarantine bloat is the main DQX failure mode in prod).

### Testing Pattern

Unit tests use `env=local` which bypasses Databricks catalog setup and mocks `WorkspaceClient`. Test transformation methods directly (e.g., `task.enrich_order(df1, df2, df3)`) using in-memory DataFrames. Integration tests use the `setup` → `run` → `validate` job sequence triggered via `make run env=dev`.

### CI/CD (GitHub Actions)

On every push: install deps → unit tests → bundle validate → deploy to staging → run integration tests → (only on `main`) deploy to prod. Requires `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, `TEMPLATE_ALERT_EMAILS` repo secrets. CLI and action versions are pinned (no `@main`).

### Production guardrails

- `databricks.yml` prod target has `mode: production` → DABs refuses to deploy if deployer != run-as identity (the SP). A developer's local `make deploy env=prod` will fail by design.
- CI deploys to prod only when `github.ref == 'refs/heads/main'`.
- `run_as` and `permissions` on every staging/prod job are pinned to the service principal's `application_id` (numeric), wired by `_get_service_principal_id` in `sdk_generate_template_job.py`.
- Prod-only features in `_build_job`: cron schedule, `JobEmailNotifications`, a `health_check` task running before any extract, and a `JobsHealthRule` on `RUN_DURATION_SECONDS > DURATION_WARNING_SECONDS` (30 min) so the `on_duration_warning_threshold_exceeded` email actually has an event to fire on.
- The wheel filename in `JobEnvironment.dependencies` is pinned to `_project_version()` (reads `pyproject.toml`) so a forgotten rebuild can't silently deploy an old wheel.
- Every job sets `max_concurrent_runs=1` + `queue.enabled=true`: late runs queue instead of getting silently skipped. Retries (staging/prod only) back off `MIN_RETRY_INTERVAL_MS` (60s). Per-task `timeout_seconds` (constants near the top of `sdk_generate_template_job.py`) prevent one hung task from eating the whole job budget. `notification_settings.no_alert_for_canceled_runs / _skipped_runs` keeps deliberate cancellations off the on-call pager.

### Adding a New Job

1. Create task classes under `src/template/<jobN>/`, inheriting `BaseTask`.
2. Register them in the `TASKS` dict in `main.py` — `--task` choices are auto-derived from `sorted(TASKS.keys())`.
3. Add task construction logic to `scripts/sdk_generate_template_job.py` (use `_wheel_task()` with no args; `--task` is filled by `{{task.name}}`).
4. Run `make deploy env=dev` to regenerate `resources/jobs.yml` and deploy.

## Constraints (things that broke us)

- **Do not call `DataFrame.cache()` / `.persist()`.** Databricks serverless rejects these with `[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported`. The double-scan cost is acceptable.
- **Do not use `assert` for runtime checks.** Python `-O` strips them. Use `if cond: raise RuntimeError(...)`.
- **Do not use `print()`.** Use `self.logger.info(...)` so output is structured and visible in the Databricks driver log. The logger handler is installed in `config.py:_configure_logging` (`template` logger, `propagate = False` to avoid py4j teardown noise).
- **Do not pass `${workspace.current_user.short_name}` as a `--user` arg.** Identity comes from `WorkspaceClient` at runtime with sanitization. If you re-add the CLI arg, you reintroduce a deploy-time/runtime mismatch.
- **`run_as` field on a job dict takes `application_id` (int), not `display_name`** — the dict key is named `service_principal_name` for legacy reasons, but the value is the numeric app ID.
- **All writes must use `.option("overwriteSchema", "false")`** on medallion tables. Schema drift is a failure signal, not something to silently absorb. The only exception is `ops._health` (intentional; `overwriteSchema=true` is fine there).

## Don't

- Don't ship changes to the CLI surface (`main.py:arg_parser`), runtime env vars, catalog/schema model, or production guardrails without updating `README.md` and this file (`CLAUDE.md`) in the same commit. Stale docs are worse than no docs — they mislead future contributors and future sessions.
- Don't reintroduce `--user`, `--debug`, or `--schema` CLI args. They were removed deliberately — see PR #21.
- Don't add `funcy` (or any decorator-based timing utility) to the dependencies. Use the structured logger.
- Don't add `CREATE CATALOG` or `CREATE SCHEMA` calls outside the `args.env == "dev"` branch in `config.py`. Staging/prod catalogs and schemas are owned by `make init`; runtime jobs run without those privileges.
- Don't commit `resources/jobs.yml` (gitignored — regenerated on every deploy).
- Don't commit `.databricks-resources.json` (gitignored — local provisioning state, diverges per developer).
- Don't hand-edit `resources/jobs.yml` — it's overwritten on every deploy. Change `scripts/sdk_generate_template_job.py` instead.
