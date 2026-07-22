# Tooling: MCP servers, CLI, and skills

This project is developed with the [Databricks AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit).
The kit is **user-level tooling**: nothing it installs belongs in this repo, and none of it is
committed. This doc is the single source of truth for what's wired up locally and what to reach for;
`CLAUDE.md` carries only a short per-session decision list that links back here. Neither doc
provisions anything.

## Install layout

The kit lives at `~/.ai-dev-kit/` — a git clone (`repo/`, pinned to a release tag), its own `.venv/`,
and installer bookkeeping (`version`, `.skills-profile`, `.installed-skills`). Currently **v0.1.13**,
profile `all`. Upgrade by re-running the installer, which refreshes every tracked root:

```bash
bash <(curl -sL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.sh)
```

Skills install into three user-level roots, one per agent tool — all fed by that one installer:

| Root | Tool |
|---|---|
| `~/.claude/skills/` | Claude Code |
| `~/.agents/skills/` | Codex |
| `~/.github/skills/` | Copilot |

**Don't install the kit into this repo.** Skills placed in a project root silently take precedence
over the user-level set *and* are invisible to `install.sh`, so they never update — the drift is
undetectable from a session. This repo carried exactly that: a project-scoped install from
2026-05-26 left `.claude/skills/`, `.github/skills/`, and `.ai-dev-kit/` here, and the stale
`.claude/skills/` shadowed the user-level set for two months. It was removed on 2026-07-16;
`.github/skills/` and `.ai-dev-kit/` remain (gitignored, stale, and only relevant to Copilot).

`.gitignore` keeps all of it out of the repo: `.mcp.json`, `.ai-dev-kit/`, `.github/skills/`, and
`.claude/*`. The one exception is `.claude/commands/`, which is un-ignored so project slash commands
(e.g. `/project-costs`) are committed and every developer gets them.

## MCP servers

Four servers are configured in `.mcp.json` (all `defer_loading: true` — schemas load on demand):

| Server | Tools | Auth / env | Reach for it when… |
|---|---|---|---|
| **databricks** | `mcp__databricks__*` (`manage_jobs`, `manage_job_runs`, `manage_uc_objects`, `execute_sql`, `manage_serving_endpoint`, `manage_workspace_files`, …) | `DATABRICKS_CONFIG_PROFILE=DEFAULT` — see the identity note below | any workspace / Unity Catalog / Jobs / Pipelines / Apps / Serving / SQL operation. **Prefer these over `databricks` CLI shell-outs or hand-rolled SDK scripts.** |
| **aws-billing-cost** | `mcp__aws-billing-cost__*` (cost-explorer, pricing, cost-anomaly, cost-comparison, budgets, …) | `AWS_PROFILE=costs` (dedicated read-only IAM user) | analyzing project cloud spend, cost anomalies/spikes, or pricing. Pairs with `scripts/project_costs.py` and `/project-costs`. Defaults: UnblendedCost, exclude credits/refunds. |
| **aws-documentation** | `mcp__aws-documentation__*` (search_documentation, read_documentation, recommend) | none | you need authoritative AWS docs (S3, IAM, external locations, Cost Explorer semantics). Cite the doc URL. |
| **context7** | `mcp__context7__*` (resolve-library-id, query-docs) | none | you need **current** docs for a library / SDK / CLI (PySpark, Databricks SDK, uv, ruff, pytest). Prefer over web search for library docs — training data may be stale. Not for refactoring or business-logic debugging. |

The `databricks` server runs out of the kit's venv
(`~/.ai-dev-kit/.venv/bin/python ~/.ai-dev-kit/repo/databricks-mcp-server/run_server.py`), so it
breaks if `~/.ai-dev-kit/` is moved or removed.

### MCP runs as the production service principal

All four profiles in `~/.databrickscfg` point at the same workspace host, but they resolve to **two
different identities** (verified with `databricks current-user me --profile <P>`):

| Profile | Auth | Resolves to |
|---|---|---|
| `dev` | `auth_type = databricks-cli` (user OAuth) | your user account |
| `DEFAULT`, `staging`, `prod` | `oauth-m2m` | the `template-sp` service principal |

The `dev` profile also carries a `client_id`/`client_secret`, but they are **inert** — `auth_type =
databricks-cli` overrides them. Comparing `client_id` values across profiles is therefore
misleading; check the resolved identity with `databricks auth describe --profile <P>` instead.

This matters because the `databricks` MCP server is pinned to `DATABRICKS_CONFIG_PROFILE=DEFAULT`,
which resolves to `template-sp` — **the same identity `prod` runs as**. MCP tool calls do not run as
your `dev` user and are not scoped to dev: an `execute_sql` through the server carries the service
principal's privileges and can read or write `prod` tables. Environment separation is *catalog-level*,
exactly as [`data-model.md`](data-model.md) describes — the guardrail is the catalog you name in the
query, not the profile you assume you're on. Name catalogs explicitly and check before mutating.

Profiles still select the bundle target where a *script* maps profile → env (`make deploy env=prod`
uses `prod`), which is what `make whoami` reports on.

If MCP tools are unavailable in a session, fall back to the `databricks` CLI or `databricks-sdk`
directly (or `aws` CLI / web search for the AWS and context7 cases) — but flag the fallback.

## Databricks CLI

Used for bundle work and as the MCP fallback. The day-to-day surface is wrapped in the `Makefile`
(`make deploy`, `make run`, `make drop`, `make init`). Use the `dev` profile unless told otherwise;
use `prod` for prod operations. To check or switch profiles, invoke the `databricks-config` skill.

## Skills

Invoke via the Skill tool when the task matches. `databricks` (frontmatter name: **`databricks-core`**)
is the kit's entry point for CLI, auth, and bundle work — load it first, then the product skill.

- **databricks-bundles** — editing `databricks.yml` / `resources/*.yml`, deploy/run. Note: this
  project **generates** `resources/jobs.yml` via `scripts/sdk_generate_template_job.py`; route job
  changes through that script + `make deploy`, never hand-edit the generated file.
- **databricks-jobs** — guidance on adding/modifying jobs (then apply via the generator above).
- **databricks-config** — switching workspaces, checking auth/profile.
- **databricks-python-sdk** — SDK code under `src/template/` and in `scripts/`.
- **databricks-unity-catalog**, **databricks-aibi-dashboards**, **databricks-spark-declarative-pipelines**,
  etc. — invoke when the task is squarely in that area.

Two gotchas. Some skills' frontmatter `name:` differs from their directory (`databricks` declares
`databricks-core`; `analyze-mlflow-trace` declares `analyzing-mlflow-trace`) — **invoke by directory
name**, which is what the session's skill list shows; the frontmatter name is not the handle. And
`/project-costs` is **not** a kit skill; it's this repo's own committed slash command
(`.claude/commands/project-costs.md`) wrapping `scripts/project_costs.py`.

`databricks-core` also cross-references skills by their *post-migration* names — it points at
`/databricks-dabs` and `databricks-data-discovery`, neither of which is installed yet. Read those as
`databricks-bundles` and "not available" until the rename below lands.

## Upcoming: skills move to the official Databricks set

v0.1.13 is the **last release that installs skills from the kit repo's own files**. The next release
installs them from the official, engineering-supported Databricks skills set via the Databricks CLI;
`install.sh` stays the front-end, so the upgrade command doesn't change. The **MCP server and Builder
App stay in the kit repo** — but the MCP server drops to *best-effort maintenance as issues are
filed*, which is worth knowing given this project depends on it daily.

Most skill names carry over. The exceptions will break references in this doc and `CLAUDE.md`, so
update both when the release lands:

| Today | Official set |
|---|---|
| `databricks-bundles` | `databricks-dabs` |
| `databricks-spark-declarative-pipelines` | `databricks-pipelines` |
| `databricks-lakebase-autoscale`, `databricks-lakebase-provisioned` | `databricks-lakebase` (merged) |
| `databricks-config` | folded into `databricks-core` |

## Conventions

- Use the `dev` profile unless told otherwise; `prod` for prod jobs/SQL/pipelines — but see
  [MCP runs as the production service principal](#mcp-runs-as-the-production-service-principal):
  MCP calls bypass your `dev` identity entirely. The catalog is the guardrail, not the profile.
- Do **not** install the Dev Kit into this repo or commit MCP config — it's user-level tooling, and
  a project-scoped install silently shadows the maintained set.
- Prefer MCP tools over CLI shell-outs when both are available; flag any fallback.
