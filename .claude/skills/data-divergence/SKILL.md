---
name: data-divergence
description: Investigate why two datasets that should agree don't — two pipelines writing the same logical table, a rollup vs the detail it aggregates, a dashboard vs its source, one environment vs another. Use when row counts, totals, or date ranges disagree and the question is what happened rather than just what differs. Covers localizing the first layer that diverges, diffing by grain and by key, reconciling across an aggregation boundary, reading Delta history and row_commit_version, why append-only tables diverge permanently, and what a fix actually costs. See examples/ for a worked investigation.
---

# Investigating a data divergence

Report what happened, not just what differs.

**Verify the premise first.** "There's a divergence between X and Y" is a hypothesis, not a finding.
Reconcile before theorizing — a good share of reports turn out to be the wrong two columns compared,
and saying so plainly with numbers is a complete and useful answer.

## Steps

1. **Establish the two sides and the grain.** Name the exact objects and the key that identifies one
   row on each side. If the two sides have *different* grain — one is a rollup of the other — jump to
   [Across an aggregation boundary](#across-an-aggregation-boundary); a row-count comparison is
   meaningless there. If the complaint came from a chart, read its query first — see
   [Dashboards](#dashboards-lie-before-tables-do).
2. **Count, at every layer.** One `UNION ALL` down both paths — source → each intermediate → output —
   with `COUNT(*)`, `MIN`/`MAX` of the partitioning/date column, and a distinct count of the key.
   Find the **first layer where the two sides stop agreeing**; that one query eliminates everything
   upstream of it.
3. **Diff by grain**, then by key (below). Stop and read the *pattern* before forming a theory.
4. **Read the Delta log** for the layer that first diverged (below).
5. **Check run state** — a missing tail is a failed job, not a logic bug. Look at job/pipeline run
   history and events, and compare the *deployed* revision against the one in source control; a fix
   that exists in the repo but was never shipped presents exactly like a code bug that isn't there.
6. **Report** what is proven, what is inferred, and what the retained logs can no longer answer.

On Databricks, drive all of this with `execute_sql` rather than shelling out. Confirm which identity
the connection authenticates as before trusting environment isolation — an MCP server or shared
service principal may reach further than your own account does.

## Diff by grain, then by key

Group both sides by the dimension the report slices on and show **only** the buckets that differ:

```sql
WITH a AS (SELECT <grain_col>, COUNT(*) n FROM <left>  GROUP BY <grain_col>),
     b AS (SELECT <grain_col>, COUNT(*) n FROM <right> GROUP BY <grain_col>)
SELECT COALESCE(a.<grain_col>, b.<grain_col>) AS g, a.n, b.n, COALESCE(b.n,0)-COALESCE(a.n,0) AS diff
FROM a FULL OUTER JOIN b USING (<grain_col>)
WHERE COALESCE(a.n,0) <> COALESCE(b.n,0) ORDER BY g
```

The shape of that output *is* the diagnosis:

- **A missing block at the tail** → a failed or not-yet-run job. Go to step 5.
- **Both edges shifted by a constant, middle identical** → the same rows under a shifted window.
  Nothing is missing; a derived column moved.
- **Scattered small deltas at one boundary** → usually the shift crossing a step in the data's own
  distribution, not a second bug. Don't chase it separately.
- **Totals equal but distribution different** → the decisive reframe. This is not "rows missing",
  it is "same rows, different values", and it points at a *column*, not at a join or a filter.

Then join on the key and histogram the delta:

```sql
SELECT <a.suspect_col> - <b.suspect_col> AS delta, COUNT(*) n   -- or datediff() for dates
FROM <left> a JOIN <right> b USING (<key>) GROUP BY 1 ORDER BY n DESC
```

One bucket holding nearly all rows means a **formula**, not corruption. Select the other columns in
the same query: if everything else is byte-identical, the join keys and the transform are exonerated
and only that one column's provenance is still in question. Confirm by tracing a single key
end-to-end through every layer of both paths.

## Across an aggregation boundary

When one side is a rollup of the other, the two don't share a grain and `COUNT(*)` comparisons say
nothing. Reconcile instead:

1. **The rollup's `GROUP BY` tuple is the grain.** Read it out of the transform, not out of the
   table's column list.
2. **Additive measures must survive.** `SUM` of the detail equals `SUM` of the rollup — check
   globally *and* per bucket, because a global total hides offsetting errors in both directions.
3. **Non-additive measures must not be summed at all.** `COUNT(DISTINCT)`, `MIN`/`MAX`, ratios and
   averages don't compose across groups. Summing a `COUNT(DISTINCT)` column over-counts whenever one
   entity spans more than one group. If it happens to reconcile anyway, that is a property of the
   current data — often because the grouping columns are functionally dependent on the entity — not a
   guarantee. Say so rather than banking it.
4. **Then full-outer join the regrouped detail to the rollup on the whole tuple** and count
   mismatches by category:

```sql
WITH s AS (SELECT <group_cols>, SUM(<measure>) m, COUNT(DISTINCT <entity>) e
           FROM <detail> GROUP BY <group_cols>)
SELECT COUNT(*) AS groups,
       COUNT(*) FILTER (WHERE g.<any_group_col> IS NULL) AS only_in_detail,
       COUNT(*) FILTER (WHERE s.<any_group_col> IS NULL) AS only_in_rollup,
       COUNT(*) FILTER (WHERE ABS(s.m - g.<measure>) > <tolerance>) AS measure_mismatch,
       COUNT(*) FILTER (WHERE s.e <> g.<entity_count>) AS entity_mismatch
FROM s FULL OUTER JOIN <rollup> g USING (<group_cols>)
```

**Check you are comparing the right column.** Most "the aggregate doesn't match" reports are a
header-level amount being compared against a sum of line-level amounts. A parent total repeated onto
each child row is multiplied by the fan-out when summed — and frequently isn't the same quantity as
the sum of its children in the first place. Establish what each column *means* before treating a gap
as a defect. Use a float tolerance on money, and beware `FloatType` vs `DoubleType` on the two sides.

## Reading the Delta log

`DESCRIBE HISTORY` answers *when and by what*, but three traps cost real time:

- **Filtering on `operation` alone hides overwrites.** An overwrite is often logged as `WRITE` with
  `operationParameters.mode = 'Overwrite'`, so `operation NOT IN ('WRITE', …)` silently drops the
  exact event you are hunting. Filter on the mode too, and page through *all* versions — the
  interesting one is rarely in the most recent page.
- **`numOutputRows` per version reconstructs the growth curve** and distinguishes a first-run full
  rebuild (one huge write) from steady incrementals (one small write per period).
- **v0's timestamp is when the table was created, not when the data began.** A `DROP` + recreate
  resets versions to 0 and erases the prior incarnation. Absence of history is not absence of
  events — say so rather than concluding nothing happened.

**`_metadata.row_commit_version` is the sharpest tool in the box**, and the one to reach for first:

```sql
SELECT _metadata.row_commit_version AS v, COUNT(*) n, MIN(<col>), MAX(<col>)
FROM <table> GROUP BY 1 ORDER BY 1
```

It attributes **live rows** to the commit that wrote them, so it pins the blast radius to specific
commits — and unlike time travel it is not bounded by retention. Seeing all the bad rows in one
commit, with the very next commit already correct, converts a theory into a fact.

Time travel does not survive: `VERSION AS OF` fails past `delta.deletedFileRetentionDuration`
(168 hours by default), and pipeline event logs age out too. Never build an investigation plan
around time-travelling a month-old event.

## Append-only semantics: why divergence becomes permanent

The load-bearing intuition. A **materialized view** is defined as a query over current inputs and
recomputes from scratch, so it self-heals when an input is re-derived. A **streaming table** — and an
insert-only `MERGE` on a batch path — appends each row once and never revisits it.

So an append-only table freezes **every column it appends**, not just the one the freeze was designed
for. A pipeline that deliberately freezes a slowly-changing attribute at append time (a name, a
country, a price) also freezes whatever else it read from the static side of that join — including
columns nobody thought of as frozen. When the upstream is later re-derived, the recomputing side
moves and the appending side cannot, and the two drift apart permanently.

Two corollaries worth stating in any report:

- **"One side fixed itself" is a clue, not a reassurance.** It usually means that side took a
  full-rebuild branch (a first-run overwrite, a full refresh), so it is *newer* — not necessarily
  *righter*.
- **Ask whether the disputed column is a stable fact at all.** Anything derived from "now" at load
  time — `current_date()`, a run-date anchor, an offset from a job parameter — silently re-derives
  itself on every reload, so a reseed rewrites history that looked immutable. That is a source bug
  wearing a pipeline divergence as a disguise, and no downstream reset fixes it permanently.

## Dashboards lie before tables do

When the report is "the dashboard doesn't match", suspect presentation before data. Check the tile's
query for a different grain, a filter the table query lacks, a `LEFT` vs `INNER` join, an implicit
`LIMIT`, and any latest-value binding — a chart that deliberately consolidates a renamed entity under
its *current* label will legitimately read differently from a table that keeps the frozen historical
label on every row. Confirm the two sides disagree at the same grain before opening the Delta log.

## When two facts contradict

If the evidence says two things that cannot both be true, an **identity assumption** is wrong, not
the evidence. In order of likelihood: the object was dropped and recreated; it is a view or
materialized view rather than a table (`DESCRIBE HISTORY` refuses views — that refusal is
information); a stale materialization was read; or the job is pointed at a different catalog or
environment than you assume. Check those before inventing a mechanism.

Say plainly which parts of the reconstruction are proven by data and which are inference from a log
that no longer reaches back far enough. A confident wrong timeline is worse than an honest gap.

## Before proposing a fix

Divergences rarely have a free repair. A full refresh of an append-only pipeline re-derives the
broken column **and** discards every frozen value it was carrying — you may be trading a divergence
in one column for a divergence in another. Say which columns move, in both directions, before
recommending it. Check the run state of the other path first too: resetting a table whose writer is
currently failing leaves it empty.

## The worked example

`examples/2026-08-06-prod-batch-vs-sdp.md` is a full investigation of this repo's own `prod` catalog,
written to the shape above: layer counts, a key-level diff, `row_commit_version` to pin the blast
radius, a Delta-history timeline, then proven-vs-inferred and costed fix options.

It is the only place this skill names real tables, and it is worth reading for what it found rather
than for the procedure. The reported complaint was one shifted date column; three further divergences
turned up, including a **live gap in a production gold table** that nobody had reported — a failed
daily run whose date-scoped incremental MERGE meant no later run ever backfilled it, while the
full-overwrite layer above it self-healed and hid the failure. Two of the four also came from a
single append-only commit freezing a column nobody intended to freeze.

Structure a report the same way, and keep the appendix of queries: the next investigation starts by
editing them rather than by rewriting them.
