# CLAUDE.md

Guidance for Claude Code when working in this repository. This is a PySpark medallion-architecture template for Databricks, packaged as a wheel and deployed via Databricks Asset Bundles (DABs).

## Project shape

- **`src/template/`** — the Python package built into the wheel.
  - `main.py` — argparse entry point (`template.main:main`). Dispatches to one task per invocation.
  - `config.py` — runtime config: catalog/schema setup, logging, DQX engine, skip-table logic.
  - `baseTask.py` — `BaseTask` with `config`, `spark`, `logger` attributes for every task.
  - `commonSchemas.py` — shared PySpark `StructType` definitions.
  - `job1/` — one file per task class (`extract_source1`, `extract_source2`, `generate_orders`, `generate_orders_agg`, `integration_setup`, `integration_validate`, `health_check`).
- **`scripts/`** — Databricks SDK / bundle automation. Run via `make` targets, not directly.
- **`resources/jobs.yml`** — **generated** from `scripts/sdk_generate_template_job.py`. Do not edit by hand; it is overwritten on every `make deploy`.
- **`tests/job1/unit_test.py`** — pytest suite (5 tests). Runs locally without a Databricks workspace via `env=local` (mocks `WorkspaceClient`).
- **`databricks.yml`** — bundle config: targets `dev`, `staging`, `prod`. Prod uses `mode: production`.

## Catalog / schema model (load-bearing)

- **Environment isolation is at the *catalog* level, not the schema level.** Same medallion schemas exist in every catalog.
- `dev_{sanitized_user}` — per-developer sandbox; created lazily by `Config.__init__`. Username is `WorkspaceClient().current_user.me().user_name.split("@")[0]` with non-alphanumerics replaced by `_` (e.g. `andre.f.salvati` → `andre_f_salvati`).
- `staging`, `prod` — shared; provisioned by `make init` (`sdk_init_workspace.py`). Runtime jobs in these envs must NOT have `CREATE CATALOG` privilege.
- Medallion schemas (`MEDALLION_SCHEMAS` in `config.py`): `external_source`, `raw`, `curated`, `report`, `ops`.
  - `ops` is the internal config schema (skip table, health check table). Named `ops` instead of `system` because Unity Catalog reserves `system`.

Each task's input/output tables are **hardcoded** in the task module (e.g. `raw.customer` → `curated.order_enriched`). The medallion layer is a semantic contract, not a runtime parameter — this is the dbt `ref()` pattern. Don't parameterize the layer; if a task genuinely needs a configurable target, that's a different task.

## CLI surface

The wheel entry point is intentionally minimal:

- `--task` *(required)* — task key (`extract_source1` etc.). In jobs, `{{task.name}}` fills this.
- `--env` *(required)* — `dev` / `staging` / `prod` (or `local` for tests).
- `--skip` *(optional flag)* — short-circuit this run.

Anything else should be an **environment variable**, not a CLI arg. Current env vars:

- `TEMPLATE_LOG_LEVEL` — `DEBUG`/`INFO`/`WARNING`. Defaults `INFO`. Overridable per-run from the Databricks Jobs UI.
- `TEMPLATE_QUARANTINE_FAIL_RATIO` — hard-fail `extract_source2` if quarantine ratio exceeds this. Defaults `1.0` (disabled).
- `TEMPLATE_ALERT_EMAILS` — CI overrides prod failure-email recipients (comma-separated).
- `TEMPLATE_SP_APP_ID` — CI bypass for the SCIM lookup of the service principal.

## Constraints (things that broke us)

- **Do not call `DataFrame.cache()` / `.persist()`.** Databricks serverless rejects these with `[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported`. The double-scan cost is acceptable.
- **Do not use `assert` for runtime checks.** Python `-O` strips them. Use `if cond: raise RuntimeError(...)`.
- **Do not use `print()`.** Use `self.logger.info(...)` so output is structured and visible in the Databricks driver log. The logger handler is installed in `config.py:_configure_logging` (`template` logger, `propagate = False` to avoid py4j teardown noise).
- **Do not pass `${workspace.current_user.short_name}` as `--user`.** Identity comes from `WorkspaceClient` at runtime (with sanitization). If you re-add the CLI arg, you reintroduce a deploy-time/runtime mismatch.
- **`run_as` field on a job dict takes `application_id` (int), not `display_name`** — the key is named `service_principal_name` for legacy reasons, but the value is the numeric app ID. See `_get_service_principal_id` in `sdk_generate_template_job.py`.
- **All writes must use `.option("overwriteSchema", "false")`** on medallion tables. Schema drift is a failure signal, not something to silently absorb. The only exception is `ops._health` (intentional, `overwriteSchema=true` is fine there).

## Common workflows

| Task | Command | Notes |
|---|---|---|
| Sync deps | `make sync` | uv-driven |
| Run unit tests | `make test` | All tests use `env=local`; mocks `WorkspaceClient`. |
| Deploy to dev | `make deploy env=dev` | Regenerates `resources/jobs.yml`, builds wheel, uploads bundle. |
| Run integration tests | `make run env=dev` | Triggers `job1_dev_integration_test` on Databricks: `setup → run (job1_dev) → validate`. |
| Lint/format | pre-commit (auto) | ruff + ruff-format; runs on commit. |
| Bootstrap workspace (one-time) | `make init` | Edit the S3 path in the Makefile first. Creates SP, catalogs, schemas, grants. |

## Production deploy is gated

- `databricks.yml` prod target has `mode: production` → DABs refuses to deploy if deployer != run-as identity (the SP). A developer's local `make deploy env=prod` will fail by design.
- CI (`.github/workflows/onpush.yml`) deploys to prod only when `github.ref == 'refs/heads/main'`.
- Prod-only features in `_build_job`: cron schedule, `JobEmailNotifications`, the `health_check` task running before any extract.

## When editing `sdk_generate_template_job.py`

- It generates `resources/jobs.yml`. Read the actual generated YAML after changes to sanity-check.
- `_wheel_task()` parameters list should stay minimal (`--task`, `--env`). Don't re-add `--user`, `--debug`, `--schema`.
- `_tags()` includes `environment`, `cost_center`, `team` for `system.billing.usage` attribution.
- The wheel filename is pinned to `_project_version()` (reads `pyproject.toml`). If you change the version, the same script run regenerates the YAML correctly.

## Don't

- Don't reintroduce `--user`, `--debug`, or `--schema` CLI args. They were removed deliberately; see the PR history for the design discussion.
- Don't add `funcy` (or any decorator-based timing utility) to the dependencies. Use the structured logger.
- Don't add `CREATE CATALOG` calls outside the `args.env == "dev"` branch in `config.py`. Staging/prod jobs run without that privilege.
- Don't commit `resources/jobs.yml` (gitignored — regenerated on every deploy).
- Don't commit `.databricks-resources.json` (gitignored — local provisioning state, diverges per developer).
