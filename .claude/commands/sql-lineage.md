Generate a column-level lineage diagram for a SQL query and explain what it shows.

## Steps

1. Get the SQL. If the user named a file, use it. If the query is embedded in Python (most of this
   repo's SQL lives in f-strings under `scripts/`), extract it to a scratch `.sql` file first and
   **replace the interpolated placeholders with literals** — `sqlglot` parses SQL, not f-strings.
2. Run `make sql-lineage sql=<path>` via Bash. It prints a Mermaid `flowchart LR` to stdout.
3. Show the diagram in a ```mermaid fence and explain it (see below).

Lineage comes from the parsed AST, so the edges are exactly what the query says — do not "improve"
the diagram by adding edges you believe should be there. If an edge looks wrong, the query is the
thing to question.

## Reading the output

- **Subgraphs are source tables**, one node per source column actually read. A column the query
  never touches does not appear — that is the point.
- **The `output` subgraph** is the projected column list, in select order.
- **`(unqualified)`** collects columns referenced without a table prefix in a multi-table join.
  `sqlglot` will not guess which side they came from without the table schemas, and neither should
  you. Call it out: it is usually a readability defect in the query worth fixing at the source.
- **Struct columns collapse to their root.** `u.usage_metadata.job_id` traces back to
  `usage_metadata`, not to the leaf field. Say so rather than implying field-level precision.

## What to say about it

- Which output columns are **derived** (aggregates, arithmetic) versus passed through unchanged.
- Any source column feeding **many** outputs — it is the query's hub, and a schema change there has
  the widest blast radius.
- Any table contributing **only one or two** columns, especially through a `LEFT JOIN`. That is
  often a lookup that could be a smaller subquery, and a join whose only job is one column is a
  cheap thing to get wrong.
- Columns in `WHERE`/`GROUP BY` but not in the output do **not** appear as nodes. If filtering
  matters to the explanation, read it off the query text and mention it separately.

## Limits worth stating rather than hiding

- `SELECT *` errors out by design — tracing it needs the table schemas, which the script does not
  have.
- Dialect defaults to `databricks`; pass `--dialect` to `scripts/sql_lineage.py` directly for others.
- Multi-statement scripts, `CREATE TABLE AS`, and CTEs resolve through to their base tables, but a
  query reading a view stops at the view name — the view's own definition is not expanded.
