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
`.claude/*`. Un-ignored so every developer gets them: `.claude/hooks/` and `.claude/settings.json`
(see [Hooks](#hooks)), and this repo's own three skills — `data-divergence`, `project-costs` and
`sql-diagram` — each named explicitly rather than by a wildcard, so the kit's skills in the same
directory stay ignored. Adding a repo skill means adding another negation pair; `git add -An
.claude/` should still stage only our files, and is the check to re-run after touching those lines.

There is no `.claude/commands/` any more: `/project-costs` and `/sql-diagram` began as slash commands
and were converted to skills, so their instructions load on relevance rather than only when typed.
Typing `/project-costs` still works — it resolves to the skill.

**`.claude/settings.local.json` stays ignored** — it is personal (permissions, machine-specific
paths, per-developer MCP toggles). Anything absolute or specific to one machine belongs there, not in
the committed `settings.json`; Claude Code merges the two.

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

## Hooks

`.claude/hooks/` holds the three shell hooks that enforce the git workflow in
[workflow.md](workflow.md). They are **committed** — the rules they enforce are stated as project
rules in `CLAUDE.md`, so shipping the scripts is what makes those statements true for a fresh clone
rather than a description of one machine's setup.

| Hook | Fires on | Effect |
|---|---|---|
| `protect-main-branch.sh` | any Bash `git commit` / `git push` | blocks a commit made while on `main`, and any push targeting `main`. |
| `require-changelog-entry.sh` | Bash `gh pr merge` | blocks the merge unless the branch diff touches `specs/CHANGELOG.md`, compared against `origin/main`/`main` via a merge-base (`...`) diff. |
| `pr-merge-description.sh` | Bash `gh pr merge` | pushes the PR title/body to GitHub, then rewrites the command with `--subject`/`--body-file` so the description becomes the merge commit message. Skips if `--body`/`--subject`/`--body-file` or `--rebase` is already present. |

The scripts are portable — no absolute paths, no secrets — and are committed mode `755`.

**The wiring is committed too.** A hook only runs if a settings file registers it, so
`.claude/settings.json` is un-ignored and registers all three as `PreToolUse` command hooks matching
`Bash`. It refers to them as `$CLAUDE_PROJECT_DIR/.claude/hooks/<name>.sh`, never as an absolute
path — that variable is what keeps the file valid in any clone, and a hardcoded path is the one edit
that would quietly break it for everyone else. Keep machine-specific hooks (an update check pointing
into `~/.ai-dev-kit/`, say) in `.claude/settings.local.json` instead; the two files are merged.

`require-changelog-entry.sh` and `protect-main-branch.sh` self-gate on the command text instead of
trusting a settings-level `if:` filter, so they stay correct however they are registered.

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
### This repo's own skills

Committed under `.claude/skills/`, and **not** part of the kit — don't expect `install.sh` to
update them, and do keep them in sync with the code they wrap.

- **data-divergence** — investigating why two datasets that should agree don't. The procedure is
  written generically (no table or column names from this project), so it covers batch vs SDP, a
  gold rollup vs the silver it aggregates, a dashboard tile vs its source, and prod vs staging
  alike; its `examples/` report is the one place it names real `prod` tables.
- **project-costs** — wraps `scripts/project_costs.py` via `make project-costs`: runs the report,
  then writes the analysis into its `## Analysis` placeholder.
- **sql-diagram** — wraps `scripts/sql_diagram.py` via `make sql-diagram`: query plan or column
  lineage as `.mmd` + `.svg`, plus how to read each mode.

Each ships a worked example showing what good output looks like, kept under
`.claude/skills/<skill>/examples/` rather than in `reports/`, because **all of `reports/` is
gitignored generated output** — that is where the tools write, and nothing there is committed. A new
example is a copy into `examples/`, never a `git add -f` out of `reports/`.

`data-divergence`'s example is the odd one out: the other two demonstrate a *format*, and a second
run would produce much the same document, so one example is enough. An investigation has no fixed
output — its example is kept for the **findings**, which are live defects in `prod`, and a second
investigation would be a second example rather than a replacement.

**An example is the artifact, never a commentary file beside it.** Both `sql-diagram` and
`project-costs` once shipped an `example.md` explaining their example; both were deleted, and in each
case the explanation belonged in one of two places — the artifact itself, or the `SKILL.md` as
guidance that applies to every run, not just to the committed one. Two files narrating one artifact
will drift, and the prose is the copy that goes stale. If an example needs a companion to be
intelligible, fix the example.

One gotcha on the kit's skills. Some have a frontmatter `name:` that differs from their directory
(`databricks` declares `databricks-core`; `analyze-mlflow-trace` declares `analyzing-mlflow-trace`)
— **invoke by directory name**, which is what the session's skill list shows; the frontmatter name
is not the handle. All three repo skills above declare a matching name, so they have no such split.

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
