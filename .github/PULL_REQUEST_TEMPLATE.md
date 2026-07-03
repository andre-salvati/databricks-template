<!--
Keep it focused (~200–400 words). See specs/workflow.md#pr-description-standard.
Update this description before merging — a hook uses it as the merge commit message body.
-->

## What?

<!-- Concise summary of what this PR changes. -->

## Why?

<!-- The problem this solves and the context/assumptions behind the approach. -->

## How?

<!-- The significant design decisions the diff alone won't make obvious. -->

## Validation?

<!-- How it was tested: env, edge cases, automated tests added/updated. -->

## Impact in prod

<!--
MANDATORY when this PR touches table schemas or data (commonSchemas.py, task modules, cluster keys).
Anticipate whether it can break production tables. Remember: every medallion write uses
overwriteSchema=false, so any schema drift hard-fails the job at runtime.
See specs/workflow.md#production-table-impact-check--schema-change-alert.
-->

- [ ] No table schema/data change — no production impact.
- [ ] **Schema change** — classify and declare the remediation:

| Change | Risk | Remediation |
|---|---|---|
| Add field | `overwriteSchema=false` rejects the new column → job fails | 🟠 `make drop env=<env> yes=--yes` (drop + recreate) or `ALTER TABLE … ADD COLUMN` before deploy |
| Remove field | Breaks downstream consumers; write fails on drift | 🔴 Communicate first; fix consumers; `make drop` + rebuild (column mapping for `DROP COLUMN`) |
| Rename field | Remove + name breakage | 🔴 Column mapping, or drop + recreate; update every consumer |
| Change type | Alters query results/casts/validations; write fails | 🔴 Drop + recreate; re-validate gold + dashboard |
| Change cluster key | Metadata-only; no data rewrite | 🟢 No downtime; note it here |

**Chosen strategy:** <!-- modify in place / drop + recreate / rebuild / leave as-is -->
