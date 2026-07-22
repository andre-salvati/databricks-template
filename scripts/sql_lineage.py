"""
Render a column-level lineage diagram (Mermaid) for a SQL query.

Lineage is derived from the parsed AST by `sqlglot`, not inferred by a model, so
the edges are exactly what the query says. Each output column is traced back to
the source columns it reads, and sources are grouped by their table.

Usage:
    python scripts/sql_lineage.py --file query.sql
    cat query.sql | python scripts/sql_lineage.py
    python scripts/sql_lineage.py --file query.sql --out diagram.mmd

Columns that cannot be attributed to a table (unqualified references in a
multi-table join, where resolving them would need the table schemas) are grouped
under `(unqualified)` rather than guessed at.
"""

import argparse
import re
import sys

import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage

UNQUALIFIED = "(unqualified)"


def output_columns(sql: str, dialect: str) -> list[str]:
    """Names of the columns the query projects, in select order."""
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    select = parsed.find(exp.Select)
    if select is None:
        raise RuntimeError("no SELECT found in the statement")
    names = [e.alias_or_name for e in select.expressions]
    if any(n == "*" for n in names):
        raise RuntimeError("SELECT * cannot be traced without table schemas; name the columns")
    return names


def source_columns(sql: str, dialect: str, column: str) -> set[tuple[str, str]]:
    """(table, column) pairs the given output column ultimately reads from."""
    found = set()
    for node in lineage(column, sql, dialect=dialect).walk():
        if node.downstream:  # only leaves are real source columns
            continue
        table = UNQUALIFIED
        if isinstance(node.source, exp.Table):
            table = exp.table_name(node.source)
        # strip the table alias qualifier: "u.sku_name" -> "sku_name"
        found.add((table, node.name.split(".", 1)[-1]))
    return found


def to_mermaid(sql: str, dialect: str) -> str:
    """Build a Mermaid flowchart of the query's column-level lineage."""
    edges = {col: source_columns(sql, dialect, col) for col in output_columns(sql, dialect)}

    # one node id per source column, grouped by table so each table is a subgraph
    tables: dict[str, list[str]] = {}
    for table, column in sorted({src for sources in edges.values() for src in sources}):
        tables.setdefault(table, []).append(column)
    ids = {(t, c): f"s{i}" for i, (t, c) in enumerate((t, c) for t, cols in tables.items() for c in cols)}

    lines = ["flowchart LR"]
    for table, columns in tables.items():
        # subgraph ids must be bare identifiers; the quoted label keeps the real name
        lines.append(f'  subgraph {re.sub(r"\W", "_", table)}["{table}"]')
        lines += [f'    {ids[(table, column)]}["{column}"]' for column in columns]
        lines.append("  end")

    lines.append('  subgraph out["output"]')
    out_ids = {col: f"o{i}" for i, col in enumerate(edges)}
    lines += [f'    {out_ids[col]}["{col}"]' for col in edges]
    lines.append("  end")

    for col, sources in edges.items():
        lines += [f"  {ids[src]} --> {out_ids[col]}" for src in sorted(sources)]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--file", help="path to a .sql file (default: read from stdin)")
    parser.add_argument("--dialect", default="databricks", help="sqlglot dialect (default: databricks)")
    parser.add_argument("--out", help="write the diagram here instead of stdout")
    args = parser.parse_args()

    sql = open(args.file).read() if args.file else sys.stdin.read()
    diagram = to_mermaid(sql, args.dialect)

    if args.out:
        with open(args.out, "w") as f:
            f.write(diagram + "\n")
        print(f"Wrote {args.out}")
    else:
        print(diagram)


if __name__ == "__main__":
    main()
