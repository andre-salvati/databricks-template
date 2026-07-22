# Development workflow

The end-to-end development lifecycle for `databricks-template`: how a change goes from an idea to a
merged PR, and the three test layers that gate it. These are the standing process rules — read this
before starting any change. For what the code does, see [architecture.md](architecture.md) and
[data-model.md](data-model.md).

## Development workflow

1. **Start with a plan.** For anything beyond a trivial one-liner, plan the approach first and get
   sign-off before writing code.
2. **Always ask "should I open a new branch?" before executing a plan.** This is a forced gate —
   never start editing without that confirmation. It stops an unrelated change from being mixed into
   a branch that is about a different subject.
3. **Never commit directly to `main`.** Cut a branch from `main` (`git checkout main && git checkout
   -b <branch>`); every change lands via PR. A local hook blocks direct commits and pushes to `main`.
4. **Hold commits until asked.** Make the edits, but don't commit until the user explicitly asks —
   the review of the working tree comes first.

## Branches & commits

- Branch from an up-to-date `main`; one subject per branch/PR. Check where you are first
  (`git branch --show-current`): if you're on `main` or on a stale/already-merged branch, run
  `git checkout main && git pull && git checkout -b <branch>` before editing. Starting from a
  diverged base causes conflicts and can silently regress work from a merged PR.
- Commit messages end with the `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>` trailer.
- **Never commit generated / local-state files** (all gitignored): `resources/jobs.yml`,
  `resources/orders_dashboard_deploy.lvdash.json`, `.databricks-resources.json`.

## PR description standard

A good PR description cuts review time — give the reviewer the scope, the reasoning, and the
decisions up front. Keep it focused (a handful of prompts, ~200–400 words), not an exhaustive
checklist and not a bare "tested locally". The standard sections are materialized in
[`.github/PULL_REQUEST_TEMPLATE.md`](../.github/PULL_REQUEST_TEMPLATE.md), which pre-populates every
new PR body:

- **What?** — a concise summary of what the PR changes.
- **Why?** — the problem it solves and the context/assumptions behind the approach.
- **How?** — the significant design decisions the diff alone won't make obvious.
- **Validation?** — how it was tested: env, edge cases, automated tests added/updated (see
  [Testing](#testing)).
- **Impact in prod** — the production-table impact check below. **Mandatory** whenever the change
  touches table schemas or data.

Operational rules:

- **Update the PR description before merging** — a hook uses the PR description as the merge commit
  message body, so whatever is in the description at merge time becomes the permanent record.
- Finalize with `gh pr edit <number> --body "..."`; merge with `--subject` +
  `--body "$(gh pr view <number> --json body -q .body)"` (never `--auto` alone).

## Production-table impact check + schema-change alert

The **final check before merge** must anticipate whether the change can break tables in production
and **raise an explicit alert in the PR**, classifying the change and declaring the remediation.
This is not merely documentation: every medallion write uses `.option("overwriteSchema", "false")`
(the schema-drift guard — the only exception is `ops._health`), so **any schema drift hard-fails the
job at runtime by design**. Schema drift is a failure signal, not something to absorb silently. The
alert in the PR is the human anticipation of that failure.

Changes that touch table schemas typically live in `src/template/commonSchemas.py` (the canonical
schemas), the task modules, or the cluster keys. Classify the change:

| Table change | Production risk | Alert / suggested remediation |
|---|---|---|
| **Add a field** | `overwriteSchema=false` **rejects** the new column (no auto-evolution here) → the job fails. | 🟠 Migrate the schema: `make drop env=<env> yes=--yes` (drop + recreate) **or** `ALTER TABLE … ADD COLUMN` before deploy. |
| **Remove a field** | Breaks downstream consumers (dashboard, queries, SDP jobs) still referencing it; the write also fails on drift. | 🔴 Communicate first; fix the dashboard/queries; `make drop` + rebuild. `ALTER TABLE … DROP COLUMN` needs column mapping enabled. |
| **Rename a field** | Same as remove, plus name breakage; watch the naming conventions in [data-model.md](data-model.md#field-naming-conventions). | 🔴 Requires column mapping, or drop + recreate; update every consumer. |
| **Change a type** | Alters query results, casts, and downstream validations; the write fails on drift. | 🔴 Drop + recreate; re-validate gold and the dashboard. |
| **Change a cluster key** | Metadata-only (`ALTER TABLE … CLUSTER BY`); does **not** rewrite data. | 🟢 No downtime; just note it in the PR. |

Declare the chosen strategy in the PR: *modify in place* / *drop + recreate* (`make drop`, the
default for schema migrations) / *rebuild* / *leave as-is*. On `staging`/`prod`, `make drop` requires
`yes=--yes`.

## CHANGELOG discipline

`specs/CHANGELOG.md` is **append-only** — add a new entry at the top before every merge, and never
edit or reformat an existing one. This section is the authority on the rule; `CLAUDE.md` only points
here.

**Write it at merge time — not before.** Don't draft the entry when you cut the branch, when you
commit, or when you open the PR. A branch's scope almost always grows, and an entry written early
just gets rewritten every time it does. Write it once, against the branch's final scope, when merge
is imminent. Forgetting isn't the risk: the `require-changelog-entry.sh` hook blocks `gh pr merge`
when the branch adds no entry, so that block is the reminder.

**Size — around 1000 characters**, written as one unwrapped paragraph. The cap is a length budget,
not a wrap width: don't hard-wrap the entry, and don't pad a short one to reach it. Character count
is the only limit — an earlier "exactly 3 sentences" rule was dropped because it constrained nothing
(three sentences had drifted into 17-line run-ons; see the first draft of #48). Name what changed
and the one fact a future reader needs — exhaustive detail belongs in the PR description and the
commit message, which is where it survives anyway.

**Header format** — use the PR URL directly, since the PR number is known once it's open:

```
## [#NN](https://github.com/andre-salvati/databricks-template/pull/NN) · YYYY-MM-DD · <title>
```

The existing entries were normalized to this style once, in #49. That was a deliberate one-off to
give the file a single voice; append-only applies from there on.

## Keep docs in sync

Ship doc updates in the **same commit** as the change. Don't touch the CLI surface
(`main.py:arg_parser`), runtime env vars, the catalog/schema model, or the production guardrails
without updating `README.md`, the relevant spec under `specs/`, and `CLAUDE.md` together. Stale docs
mislead future contributors and future sessions.

---

## Testing

Three layers of tests: **unit** (fast, local, no Databricks), **integration** (seed → run →
validate, on a real catalog), and **load** (production-scale volumes). For the schemas these tests
assert against, see [data-model.md](data-model.md).

### Unit tests — `tests/job1/unit_test.py`, `unit_test_sdp.py`

Run with `env=local`, which bypasses Databricks catalog setup and mocks `WorkspaceClient`, so tests
run with no connectivity. Transformation methods are tested **directly** on in-memory DataFrames
(e.g. `task.enrich_order(df1, df2, df3)`) rather than through Spark tables.

Run all: `make unit-test` (pytest with coverage). Single file/test:

```
uv run pytest tests/job1/unit_test.py
uv run pytest tests/job1/unit_test.py::test_enrich_orders
```

Coverage: `make unit-test` writes a report under `coverage_reports/` (uploaded as a CI artifact,
14-day retention).

| Test (batch — `unit_test.py`) | Asserts |
|---|---|
| `test_arg_parser` | CLI parsing; `--task` choices derive from `TASKS`. |
| `test_config` | `Config` resolves catalog/values per env. |
| `test_validate_orders_from_source` | DQX split — valid rows pass, bad rows quarantine. |
| `test_enrich_orders` | silver enrichment (joins + frozen `product_name`). |
| `test_aggregate_orders` | gold aggregation; `total_value = SUM(item_total)`. |
| `test_seed_sources_*` (7) | seed incremental order/item/customer/name schemas; name updates deterministic per date and follow the `Product <id>.<k>` suffix; incremental ids unique across days. |

| Test (SDP — `unit_test_sdp.py`) | Asserts |
|---|---|
| `test_enrich_order_row_count` / `_columns` | SDP silver join row count + output columns. |
| `test_aggregate_orders_row_count` / `_values` | SDP gold aggregation count + values. |

### Integration tests — `setup` → `run` → `validate`

Triggered as a Databricks job sequence via `make run env=dev` (or `staging`). The integration job is
generated by `_build_job_integration_test` in `sdk_generate_template_job.py` and threads a
`load_test` job parameter (default `false`) into the tasks.

- **`setup`** (`integration_setup.py`) — wipes and recreates all `MEDALLION_SCHEMAS` (on
  staging/prod, `Config` no longer creates schemas at runtime, so `setup` restores the layout), then
  seeds `external_source` and applies clustering keys.
- **`run`** — executes `job1` (batch) and `job1_sdp` (pipeline) over the seeded data.
- **`validate`** (`integration_validate.py`) — asserts the gold tables, checking **both**
  `report.order_agg` and `report.order_agg_sdp`.

#### Standard mode

Seeds 2 customers, 3 products, and orders that deliberately include DQX edge cases (a `total > 1000`
WARN row, a null `id`, duplicate `id`s). Validate expects exactly **2 gold rows** with
`total_value = SUM(item_total)`: John Doe `$50 + $50 = $100`, Jane Smith `$151`.

### Load tests

Same `setup`/`validate` tasks with `load_test=true`, exercising both the initial bulk load and
incremental daily updates at production scale. Seeds **500 customers × 100 products**, **2M orders**,
**6M order_items** (3 items/order). With `item_total=50` and `qty=2`, each (customer, product) group
lands at `total_quantity=240`, `total_value=6,000.0`, `total_orders=40`. Validate expects **50,000
gold rows** (500 × 100) in both `report.order_agg` and `report.order_agg_sdp`, and fails if any row
deviates from those expected aggregates.

### Where tests run

| Layer | Trigger | Env | Databricks needed |
|---|---|---|---|
| Unit | `make unit-test` / pytest | `local` | no (mocked) |
| Integration | `make run env=dev` (also staging in CI) | `dev` / `staging` | yes |
| Load | integration job with `load_test=true` | `staging` | yes |

CI runs unit tests on every push and the staging integration test after deploying to staging
(see [architecture.md](architecture.md#cicd)).
