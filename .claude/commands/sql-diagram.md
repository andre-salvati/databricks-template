Diagram a SQL query and explain what it shows — either its execution steps or its column lineage.

## Steps

1. Get the SQL. If the user named a file, use it. If the query is embedded in Python (most of this
   repo's SQL lives in f-strings under `scripts/`), extract it to a scratch `.sql` file first and
   **replace the interpolated placeholders with literals** — `sqlglot` parses SQL, not f-strings.
2. Pick the mode. `mode=plan` (the default) answers "what does this query *do*, step by step";
   `mode=lineage` answers "where does this output column come from". When the user asks about
   joins, stages, filters or ordering, they want `plan`.
3. Run `make sql-diagram sql=<path> name=<basename> comments=1` via Bash. It writes three files to
   `reports/sql-diagram/`: `<basename>.sql` (the query as analysed), `.mmd` and `.svg` — all
   gitignored, so `git add -f` them only if they are meant to be a committed example. Pass
   `--stdout` to `scripts/sql_diagram.py` for a throwaway look with no files written.
4. Read the `.mmd`, show it in a ```mermaid fence, and explain it (see below). The `.svg` is the
   same graph for linking from prose where no Mermaid renderer is available.

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
- **This is the logical plan, not the physical one.** Databricks reorders joins, chooses broadcast
  versus shuffle, and prunes columns. Say "as written" — and if the real execution matters, point
  at the query profile in the UI or `EXPLAIN FORMATTED`, which is the only authority on what ran.
- `AGGREGATE` may show synthetic operand names (`_a_0`) for `DISTINCT`/expression arguments that
  `sqlglot` lifted out. Read the intent off the original SQL rather than repeating the placeholder.

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
- `--comments` is the only part that touches the network, and it uses the `dev` profile: the MCP
  service principal lacks `USE SCHEMA` on `system.billing`.
