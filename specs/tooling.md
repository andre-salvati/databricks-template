# Tooling: MCP servers, CLI, and skills

This project is developed with the [Databricks AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit)
installed at the user level (`~/.ai-dev-kit/`). MCP servers and skills are **user-level tooling** —
`.mcp.json` and `.claude/` are gitignored and are **not** part of the repo. The one exception is
`.claude/commands/`, which `.gitignore` un-ignores: project-specific slash commands (e.g.
`/project-costs`) are committed so every developer gets them. This doc records what's wired up
locally so a session knows what to reach for; it does not provision anything.

`CLAUDE.md` carries a short decision list for every session; this is the full reference.

## MCP servers

Four MCP servers are configured in `.mcp.json` (all `defer_loading: true` — schemas load on demand):

| Server | Tools | Auth / env | Reach for it when… |
|---|---|---|---|
| **databricks** | `mcp__databricks__*` (`manage_jobs`, `manage_job_runs`, `manage_uc_objects`, `execute_sql`, `manage_serving_endpoint`, `manage_workspace_files`, …) | `dev` profile in `~/.databrickscfg` | any workspace / Unity Catalog / Jobs / Pipelines / Apps / Serving / SQL operation. **Prefer these over `databricks` CLI shell-outs or hand-rolled SDK scripts.** |
| **aws-billing-cost** | `mcp__aws-billing-cost__*` (cost-explorer, pricing, cost-anomaly, cost-comparison, budgets, …) | `AWS_PROFILE=costs` (dedicated read-only IAM user) | analyzing project cloud spend, cost anomalies/spikes, or pricing. Pairs with `scripts/project_costs.py` and the `/project-costs` skill. Defaults: UnblendedCost, exclude credits/refunds. |
| **aws-documentation** | `mcp__aws-documentation__*` (search_documentation, read_documentation, recommend) | none | you need authoritative AWS docs (S3, IAM, external locations, Cost Explorer semantics). Cite the doc URL. |
| **context7** | `mcp__context7__*` (resolve-library-id, query-docs) | none | you need **current** docs for a library / SDK / CLI (PySpark, Databricks SDK, uv, ruff, pytest). Prefer over web search for library docs — training data may be stale. Not for refactoring or business-logic debugging. |

If MCP tools are unavailable in a session, fall back to the `databricks` CLI or `databricks-sdk`
directly (or `aws` CLI / web search for the AWS/context7 cases) — but flag the fallback to the user.

## Databricks CLI

Used for bundle work and as the MCP fallback. The day-to-day surface is wrapped in the `Makefile`
(`make deploy`, `make run`, `make drop`, `make init`). Use the `dev` profile unless told otherwise;
use `prod` for prod operations. To check or switch profiles, invoke the `databricks-config` skill.

## Skills

Invoke via the Skill tool when the task matches:

- **databricks-bundles** — editing `databricks.yml` / `resources/*.yml`, deploy/run. Note: this
  project **generates** `resources/jobs.yml` via `scripts/sdk_generate_template_job.py`; route job
  changes through that script + `make deploy`, never hand-edit the generated file.
- **databricks-jobs** — guidance on adding/modifying jobs (then apply via the generator above).
- **databricks-config** — switching workspaces, checking auth/profile.
- **databricks-python-sdk** — SDK code under `src/template/` and in `scripts/`.
- **databricks-unity-catalog**, **databricks-aibi-dashboards**, **databricks-spark-declarative-pipelines**,
  etc. — invoke when the task is squarely in that area.
- **project-costs** — run `scripts/project_costs.py` and analyze the output for spikes/trends.

## Conventions

- Use the `dev` profile unless told otherwise; `prod` for prod jobs/SQL/pipelines.
- Do **not** install the Dev Kit into this repo or commit MCP config — it's user-level tooling.
- Prefer MCP tools over CLI shell-outs when both are available; flag any fallback.
