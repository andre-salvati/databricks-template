---
name: sql-diagram
description: Diagram a SQL query and explain what it shows — either its execution steps (mode=plan) or its column lineage (mode=lineage). Use when asked to visualize, diagram, explain or review what a query does, how it joins its tables, or where an output column comes from. Wraps `make sql-diagram`, which emits .mmd and .svg into reports/sql-diagram/. See example.md for a worked reading of a committed diagram.
---

# Diagramming a SQL query

Diagram a SQL query and explain what it shows — either its execution steps or its column lineage.

## Steps

1. Get the SQL. If the user named a file, use it. If the query is embedded in Python (most of this
   repo's SQL lives in f-strings under `scripts/`), extract it to a scratch `.sql` file first and
   **replace the interpolated placeholders with literals** — `sqlglot` parses SQL, not f-strings.
2. Pick the mode. `mode=plan` (the default) answers "what does this query *do*, step by step";
   `mode=lineage` answers "where does this output column come from". When the user asks about
   joins, stages, filters or ordering, they want `plan`.
3. Run `make sql-diagram sql=<path> name=<basename> comments=1` via Bash. It writes three files to
   `reports/sql-diagram/`: `<basename>.sql` (the query as analysed), `.mmd` and `.svg`. All of
   `reports/` is gitignored generated output — never `git add -f` out of it. To keep a diagram as a
   committed example, copy the trio into `.claude/skills/sql-diagram/examples/`. Pass `--stdout` to
   `scripts/sql_diagram.py` for a throwaway look with no files written.
4. Read the `.mmd`, show it in a ```mermaid fence, and explain it (see below). The `.svg` is the
   same graph for linking from prose where no Mermaid renderer is available.

**Explaining a query and reviewing one are different jobs.** For an explanation the diagram is
enough. For a *review*, read the `.sql` alongside it and treat the graph as an index into the text:
it is authoritative on scans, joins and join predicates, and silent on filters, windows and
projections (see the blind spots below). Defects severe enough to produce wrong numbers usually live
in exactly the parts the picture omits.

The emitted `.sql` is what makes the diagram auditable: it is the query *after* any f-string
placeholders were filled in, so `make sql-diagram sql=reports/sql-diagram/<basename>.sql` reproduces
the diagram exactly. When you commit a diagram as an example, commit its `.sql` with it.

Both diagrams come from the parsed AST, so they are exactly what the query says — do not "improve"
one by adding a node or edge you believe should be there. If it looks wrong, the query is the thing
to question.

## Reading `mode=plan`

Nodes are the query's steps, bottom-up: `SCAN` per table, one `JOIN n` per individual join, then
`WHERE`, `AGGREGATE`, `SORT`, `OUTPUT`. A CTE appears as its own sub-pipeline feeding the `SCAN`
that reads it.

- **Each join is numbered in the order the query writes it** and carries its side and keys.
  `sqlglot` models a multi-table join as one n-ary step; the script splits it back apart. An
  `INNER JOIN` silently drops rows where a `LEFT JOIN` keeps them — always say which, because it
  changes what a blank in the output means.
- **Extra `ON` predicates beyond the equality keys** are listed under the keys as `and …`. On a
  slowly-changing dimension those range predicates are what stop the join fanning out; call them
  out rather than treating them as noise.
- **Window functions are not drawn.** A CTE whose job is a `ROW_NUMBER()` / `RANK()` / `LAG()`
  collapses to a bare `SCAN <cte>` with no node for the window, so its `PARTITION BY` / `ORDER BY`
  are invisible — and a downstream `WHERE rank <= n` then filters on a column with no visible origin
  anywhere in the graph. Whenever a rank, top-N or dedupe is involved, read the `PARTITION BY` off
  the `.sql` and state it; the picture cannot show whether the ranking is right.
- **`WHERE` is only drawn when it sits above a join.** Filters inside a CTE, and the `WHERE` of a
  join-free query, do not appear; neither does `LIMIT`. `HAVING` appears disguised as a synthetic
  `… AS _h` line inside the `AGGREGATE` node, and `QUALIFY` not at all. Never conclude "this scans
  the whole table" or "there is no date filter" from the graph — count the filters in the `.sql` and
  say how many the diagram omitted.
- **`OUTPUT` lists aliases, never expressions.** A `COALESCE(rate, 1.0)` default, a cast or a
  division in the select list shows only as its output name. Read the projection list from the
  `.sql` and call out any silent default — that is where a broken join stops being a visibly empty
  column and starts being a confidently wrong number.
- **This is the logical plan, not the physical one.** Databricks reorders joins, chooses broadcast
  versus shuffle, and prunes columns. Say "as written" — and if the real execution matters, point
  at the query profile in the UI or `EXPLAIN FORMATTED`, which is the only authority on what ran.
- `AGGREGATE` may show synthetic operand names (`_a_0`) for `DISTINCT`/expression arguments that
  `sqlglot` lifted out. Read the intent off the original SQL rather than repeating the placeholder —
  `COUNT(\`_a_0\`)` and `COUNT(DISTINCT …)` are indistinguishable in the picture.

## Reading `mode=lineage`

- **Subgraphs are source tables**, one node per source column actually read. A column the query
  never touches does not appear — that is the point.
- **The `output` subgraph** is the projected column list, in select order.
- **`(unqualified)`** collects columns referenced without a table prefix in a multi-table join.
  `sqlglot` will not guess which side they came from without the table schemas, and neither should
  you. Call it out: it is usually a readability defect in the query worth fixing at the source.
- **Struct columns collapse to their root.** `u.usage_metadata.job_id` traces back to
  `usage_metadata`, not to the leaf field. Say so rather than implying field-level precision.
- Columns in `WHERE`/`GROUP BY` but not in the output do **not** appear. Use `mode=plan` when
  filtering is the point.

## In either mode

**The grey line under a table name** is its Unity Catalog comment, present only when the run passed
`comments=1` and the profile could read the table. It is fetched, never written by you — if a table
has no comment the space is blank, and that absence is itself worth reporting.

## What to say about it

- The **shape** of the query first: how many tables, how many joins, what it groups by. A reader
  who cannot restate the query after your first paragraph has learned nothing.
- Any source column or table feeding **many** outputs — the query's hub, where a schema change has
  the widest blast radius.
- Any table contributing **only one or two** columns, especially through a `LEFT JOIN`. That is
  often a lookup that could be a smaller subquery, and a join whose only job is one column is a
  cheap thing to get wrong.
- Join predicates that look under-constrained. A join on a slowly-changing dimension without a
  time-range predicate fans rows out and silently multiplies aggregates — this repo has been bitten
  by exactly that (see the `#47` entry in `specs/CHANGELOG.md`).
- **What each join key *means*, not just that it exists.** The plan normalizes predicates, so a
  join written `c.id = r.product_id` prints as `r.product_id = c.id`, and an equality between two
  integer ids looks correct no matter which entities they identify. Pull both schemas
  (`mcp__databricks__get_table_stats_and_schema`) and compare domains and value ranges. A
  customer-id-to-product-id join can match **100% of rows** — no NULLs, no fan-out, no error — and
  hand back a plausible, entirely fabricated dimension column.
- **The grain, against the dimensions hung off it.** If the pipeline aggregates to `A × B` and then
  joins a dimension that varies *within* `A × B`, either that dimension is fabricated or the join
  fans out and inflates every measure. The `AGGREGATE` node's `GROUP BY` line is where to check.

See `example.md` for a committed diagram read end to end, including what each of these points looks
like when it actually fires.

## Limits worth stating rather than hiding

- `SELECT *` errors out in lineage mode by design — tracing it needs the table schemas, which the
  script does not have. Plan mode draws it fine.
- Lineage mode also refuses a query whose output projects the same column name twice
  (`SELECT a.id, b.id`): `sqlglot` resolves lineage by name and would trace both to the first
  match, drawing a confident wrong graph. Alias them, or use plan mode.
- `CREATE TABLE … AS SELECT` and `INSERT … SELECT` are unwrapped to their SELECT and diagrammed.
  Anything with no SELECT at all (a `DELETE`, a DDL statement) exits with a one-line message.
- Dialect defaults to `databricks`; pass `--dialect` to `scripts/sql_diagram.py` directly for others.
- CTEs resolve through to their base tables, but a query reading a **view** stops at the view name;
  the view's own definition is not expanded.
- **CTE scans use the same cylinder as base tables** — only the name tells them apart, and a CTE
  that merely adds a window has a producing pipeline contributing nothing visible, so it reads as a
  table. A `VALUES` CTE renders as two chained identical scans.
- SQL comments leak into node labels as truncated `/* …` fragments; strip them before diagramming
  if the labels get noisy.
- `--comments` is the only part that touches the network, and it uses the `dev` profile: the MCP
  service principal lacks `USE SCHEMA` on `system.billing`.
