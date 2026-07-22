# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A production-ready PySpark/Databricks ETL pipeline template using medallion architecture, Python packaging, unit + integration tests, Databricks Declarative Automation Bundles (DABs), and DQX data quality framework. Code is structured as a Python wheel package (not notebooks) deployed to Databricks serverless.

## Tooling: MCP servers, CLI, skills → see [`specs/tooling.md`](specs/tooling.md)

Developed with the [Databricks AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit) — **user-level tooling** (`~/.ai-dev-kit/`), never installed into or committed to this repo ([why that matters](specs/tooling.md#install-layout)). Quick decision list (full reference in [`specs/tooling.md`](specs/tooling.md)):

- **Workspace / UC / Jobs / Pipelines / Apps / Serving / SQL** → prefer `mcp__databricks__*` tools over `databricks` CLI shell-outs or hand-rolled SDK scripts ([servers](specs/tooling.md#mcp-servers)).
- **Bundle / job changes** → `databricks-bundles` / `databricks-jobs` skills, and route job edits through `scripts/sdk_generate_template_job.py` + `make deploy` ([skills](specs/tooling.md#skills)).
- **Library/SDK docs** (PySpark, Databricks SDK, uv, ruff) → `context7` MCP, not memory or web search.
- **Cloud spend / cost analysis** → `aws-billing-cost` MCP (`AWS_PROFILE=costs`) + `/project-costs`. **AWS docs** → `aws-documentation` MCP.
- Use the `dev` profile unless told otherwise (`prod` for prod ops). If MCP tools are unavailable, fall back to CLI/SDK and flag it.
- **MCP calls run as the prod SP, not as you** — `dev` is your user account, but the `databricks` MCP server is pinned to `DEFAULT`, which resolves to the same `template-sp` that `prod` uses. It can read/write `prod` tables; the catalog is the guardrail ([why](specs/tooling.md#mcp-runs-as-the-production-service-principal)).

## Commands

```bash
make sync              # Install all dependencies via uv
make unit-test         # Run pytest with coverage
make pre-commit        # Update and run pre-commit hooks (ruff lint/format)
make init              # One-time workspace bootstrap (SP, catalogs, schemas, grants). Edit S3 path first.
                       # If workspace has >1 SQL warehouse, pass --warehouse-name to the underlying script.
make deploy env=dev    # Generate resources/jobs.yml (jobs + SDP pipeline) + deploy bundle to target env (dev/staging/prod)
make run env=dev       # Run integration test job on a target env (dev or staging)
make drop env=dev      # Drop all medallion tables in a target env (schema migrations; staging/prod need yes=--yes)
make whoami            # Print the identity the env's profile authenticates as (runs implicitly before deploy/run/drop)
make project-costs     # AWS + Databricks spend report (--aws-profile costs); backs the /project-costs skill
make sql-lineage sql=q.sql  # Column-level lineage diagram (Mermaid) for a SQL query; backs /sql-lineage
make star-history      # Regenerate the README star-history SVGs (assets/star_history*.svg) from the GitHub API
```

Run a single test file:
```bash
uv run pytest tests/job1/unit_test.py
```

Run a single test by name:
```bash
uv run pytest tests/job1/unit_test.py::test_enrich_orders
```

## Architecture & data model → see `specs/`

The detailed specs live in [`specs/`](specs/) — read the relevant one **before** working in that area:

- [`specs/architecture.md`](specs/architecture.md) — execution flow, CLI surface, key classes, jobs DAG, job **generation**, CI/CD, job-level params, deploy-time env vars, logging, production guardrails, adding a new job.
- [`specs/data-model.md`](specs/data-model.md) — plain-words pipeline overview, catalog/schema isolation, medallion flow, table schemas, **field naming conventions**, product-name freeze, liquid clustering, DQX/quarantine, lineage.
- [`specs/workflow.md`](specs/workflow.md) — the development lifecycle (plan → branch → PR), PR description standard, production-table impact check, and the unit / integration / load test plan.
- [`specs/tooling.md`](specs/tooling.md) — MCP servers (Databricks, AWS billing/docs, context7), CLI, and skills: what to reach for and when.
- The AI/BI dashboard: latest-name binding and deploy mechanics are documented in [`specs/data-model.md#dashboard`](specs/data-model.md#dashboard) — edit the committed `resources/orders_dashboard.lvdash.json` (catalog `${var.catalog}`, resolved at deploy time).

### Load-bearing invariants (keep in mind; full detail in specs)

- **Catalog-level isolation** — env separation is at the *catalog* level (`dev_<user>` / `staging` / `prod`); the same medallion schemas (`external_source`/`raw`/`curated`/`report`/`ops`) exist in each. Staging/prod catalogs+schemas are owned by `make init`, not the runtime wheel.
- **Product-name freeze** — silver freezes `product_name` onto each order line at sale time: batch via an insert-only `MERGE`, SDP via a **streaming table** (a materialized view would *restate* the name; a streaming table appends once and *freezes*). A later rename never relabels booked orders. `unit_price` is static; `total_value` in gold is `SUM(item_total)`.
- **Generated job config** — `resources/jobs.yml` is generated by `scripts/sdk_generate_template_job.py`; never hand-edit it (it's gitignored).

## Constraints (things that broke us)

- **Do not call `DataFrame.cache()` / `.persist()`.** Databricks serverless rejects these with `[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported`. The double-scan cost is acceptable.
- **Do not use `assert` for runtime checks.** Python `-O` strips them. Use `if cond: raise RuntimeError(...)`.
- **Do not use `print()`.** Use `self.logger.info(...)` so output is structured and visible in the Databricks driver log. The logger handler is installed in `config.py:_configure_logging` (`template` logger, `propagate = False` to avoid py4j teardown noise).
- **Do not pass `${workspace.current_user.short_name}` as a `--user` arg.** Identity comes from `WorkspaceClient` at runtime with sanitization. If you re-add the CLI arg, you reintroduce a deploy-time/runtime mismatch.
- **`run_as` field on a job dict takes `application_id` (int), not `display_name`** — the dict key is named `service_principal_name` for legacy reasons, but the value is the numeric app ID.
- **All writes must use `.option("overwriteSchema", "false")`** on medallion tables. Schema drift is a failure signal, not something to silently absorb. The only exception is `ops._health` (intentional; `overwriteSchema=true` is fine there).

## Git Workflow → full detail in [`specs/workflow.md`](specs/workflow.md)

- **Ask "should I open a new branch?" before executing a plan**, and **never commit directly to `main`** — cut a feature branch and land via PR (a hook blocks direct commits and pushes to `main`).
- **Hold commits until asked.** Before merging, update the PR description (a hook uses it as the merge commit message body) following the What / Why / How / Validation / **Impact in prod** template in [`.github/PULL_REQUEST_TEMPLATE.md`](.github/PULL_REQUEST_TEMPLATE.md); any table schema/data change needs the production-table impact check.
- **Keep docs in sync in the same commit** — the CLI surface (`main.py:arg_parser`), runtime env vars, catalog/schema model, and production guardrails each need `README.md` + the relevant `specs/` doc + this file updated together ([full rule](specs/workflow.md#keep-docs-in-sync)).
- **Add a `specs/CHANGELOG.md` entry immediately before merging** — never earlier; append-only; one unwrapped paragraph, ~1000 characters ([full rule](specs/workflow.md#changelog-discipline)).

## Keep It Simple

Favor solutions with less code, fewer classes, and fewer abstractions. When two approaches both solve the problem, prefer the one with fewer moving parts — even if the "cleaner" architecture feels more elegant. Extend existing classes before creating new ones. Add a parameter before adding a new task key. Branch on a flag before splitting into subclasses.

- Don't reintroduce `--user`, `--debug`, or `--schema` CLI args. They were removed deliberately — see PR #21.
- Don't add `funcy` (or any decorator-based timing utility) to the dependencies. Use the structured logger.
- Don't add `CREATE CATALOG` or `CREATE SCHEMA` calls outside the `args.env == "dev"` branch in `config.py`. Staging/prod catalogs and schemas are owned by `make init`; runtime jobs run without those privileges.
- Don't commit or hand-edit `resources/jobs.yml` — it's gitignored and overwritten on every deploy. Change `scripts/sdk_generate_template_job.py` instead (it generates jobs, the SDP pipeline, and the dashboard resource stanza).
- Don't commit `.databricks-resources.json` (gitignored — local provisioning state, diverges per developer).
- Don't commit or hand-edit `resources/orders_dashboard_deploy.lvdash.json` (gitignored — regenerated on every deploy). Edit the committed `resources/orders_dashboard.lvdash.json` instead — mechanics in [`specs/data-model.md#dashboard`](specs/data-model.md#dashboard).
- Don't hand-edit `assets/star_history*.svg` — they're generated (and committed, unlike `resources/jobs.yml`) so the README renders from this repo instead of a third-party chart service. Regenerate with `make star-history`; change `scripts/star_history.py` to alter the chart.
- For PySpark transformation chains, let `ruff format` shape any chain that's already multi-line (don't hand-tune it) and keep chains that fit on one line unbroken — see the convention in [`specs/architecture.md`](specs/architecture.md#code-style-pyspark-transformation-chains).
