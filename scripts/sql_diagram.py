"""
Diagram a SQL query, in two modes, from the AST that `sqlglot` parses — never from model
inference, so what is drawn is exactly what the query says.

    --mode plan     (default) the query's steps in order: one node per scan, one per *individual*
                    join with its type and keys, then the filter, aggregate and sort. CTEs appear
                    as their own sub-pipeline feeding the scan that reads them.
    --mode lineage  which source column feeds each output column.

Writes three files to `reports/sql-diagram/`: the `<name>.sql` that was analysed, `<name>.mmd`
(renders on GitHub, diffable) and a standalone `<name>.svg` for linking from prose where no
Mermaid renderer is available. Keeping the query beside the diagram is what makes the diagram
checkable — and regenerable.

Usage:
    python scripts/sql_diagram.py --file query.sql
    cat query.sql | python scripts/sql_diagram.py --name job_costs --comments
    python scripts/sql_diagram.py --file query.sql --mode lineage --stdout

In lineage mode, columns that cannot be attributed to a table (unqualified references in a
multi-table join, where resolving them would need the table schemas) are grouped under
`(unqualified)` rather than guessed at.

`--comments` additionally annotates each source table with its Unity Catalog comment. That is the
only part of this script that touches the network, so it is opt-in; a table without a comment, or
one the profile cannot read, is simply left unannotated rather than described from guesswork.
"""

import argparse
import re
import sys
from dataclasses import dataclass
from html import escape
from pathlib import Path

import sqlglot
from sqlglot import exp, planner
from sqlglot.lineage import lineage

UNQUALIFIED = "(unqualified)"
FONT = "-apple-system, BlinkMacSystemFont, Segoe UI, Helvetica, Arial, sans-serif"
OUT_DIR = Path(__file__).resolve().parent.parent / "reports" / "sql-diagram"


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


@dataclass
class Stage:
    """One step of the logical plan: a scan, a single join, a filter, an aggregate or a sort."""

    id: str
    kind: str
    title: str
    details: list[str]
    deps: list[str]


def build_plan(sql: str, dialect: str) -> list[Stage]:
    """
    Flatten sqlglot's logical plan into ordered stages.

    sqlglot models a multi-table join as one n-ary step; this splits it back into "JOIN 1",
    "JOIN 2", … in the order the query writes them, which is the order a reader thinks in. The
    result is the *logical* plan — what the SQL says. It is not the physical plan: Databricks is
    free to reorder joins, broadcast a side, or prune columns, and `EXPLAIN FORMATTED` against a
    warehouse is the only thing that shows what actually ran.
    """
    stages: list[Stage] = []
    seen: dict[int, str] = {}

    def emit(kind: str, title: str, details: list[str], deps: list[str]) -> str:
        stages.append(Stage(f"n{len(stages)}", kind, title, details, deps))
        return stages[-1].id

    def sql_of(node) -> str:
        return node if isinstance(node, str) else node.sql(dialect=dialect)

    def ordered(deps) -> list:
        # planner.Step.dependencies is a set, so iteration order changes with the hash seed —
        # without this the node numbering churns on every run and the .mmd is not diffable.
        return sorted(deps, key=lambda d: (d.name or "", type(d).__name__))

    def visit(step) -> str:
        if id(step) in seen:
            return seen[id(step)]

        if isinstance(step, planner.Join):
            by_name = {dep.name: visit(dep) for dep in ordered(step.dependencies)}
            current = by_name[step.name]  # the FROM table; every join hangs off it in turn
            for i, (source, ctx) in enumerate(step.joins.items(), 1):
                details = [
                    f"{sql_of(a)} = {sql_of(b)}" for a, b in zip(ctx.get("source_key") or [], ctx.get("join_key") or [])
                ]
                # sqlglot pads the residual predicate with TRUE AND …; drop that, keep the rest
                extra = re.sub(r"\bTRUE AND\b", "", sql_of(ctx["condition"])).strip() if ctx.get("condition") else ""
                if extra and extra != "TRUE":
                    details += [f"and {part}" for part in _split_and(extra)]
                current = emit(
                    "JOIN", f"JOIN {i} · {(ctx.get('side') or 'INNER').upper()} · {source}", details, [current]
                )
                stages[-1].deps.append(by_name[source])
            if step.condition:
                current = emit("FILTER", "WHERE", _split_and(sql_of(step.condition)), [current])
            seen[id(step)] = current
            return current

        deps = [visit(dep) for dep in ordered(step.dependencies)]
        if isinstance(step, planner.Scan):
            node = emit("SCAN", f"SCAN {sql_of(step.source).split(' AS ')[0]}", [], deps)
        elif isinstance(step, planner.Aggregate):
            group = [sql_of(g) for g in (step.group or {}).values()]
            details = ([f"GROUP BY {', '.join(group)}"] if group else []) + [sql_of(a) for a in step.aggregations]
            node = emit("AGGREGATE", "AGGREGATE", details, deps)
        elif isinstance(step, planner.Sort):
            node = emit("SORT", "SORT", [sql_of(k) for k in step.key], deps)
        else:
            node = emit("STEP", type(step).__name__.upper(), [], deps)

        seen[id(step)] = node
        return node

    root = planner.Plan(sqlglot.parse_one(sql, dialect=dialect)).root
    last = visit(root)
    emit("OUTPUT", "OUTPUT", [e.alias_or_name for e in root.projections], [last])
    return stages


def _split_and(condition: str) -> list[str]:
    """Break a conjunction onto its own lines; nested parens stay whole."""
    parts, depth, current = [], 0, ""
    for token in re.split(r"(\s+AND\s+|\(|\))", condition):
        depth += token.count("(") - token.count(")")
        if re.fullmatch(r"\s+AND\s+", token) and depth == 0:
            parts.append(current.strip())
            current = ""
        else:
            current += token
    parts.append(current.strip())
    return [p for p in parts if p]


def _shorten(text: str, limit: int) -> str:
    """Truncate on a word boundary so a long UC comment cannot blow out the layout."""
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    return text[:limit].rsplit(" ", 1)[0] + "…"


def _mermaid_safe(text: str) -> str:
    """Quoted Mermaid labels cannot contain a raw double quote."""
    return text.replace('"', "#quot;")


def _wrap(text: str, width: int, max_lines: int) -> list[str]:
    """Greedy wrap for SVG text, which has no automatic line breaking."""
    words = text.split()
    lines, current = [], []
    for word in words:
        if current and len(" ".join(current + [word])) > width:
            lines.append(" ".join(current))
            current = []
            if len(lines) == max_lines:
                break
        current.append(word)
    if current and len(lines) < max_lines:
        lines.append(" ".join(current))
    if lines and len(" ".join(lines).split()) < len(words):
        lines[-1] = lines[-1].rstrip(".,;") + " …"  # say it was cut rather than end mid-sentence
    return lines


def extract_joins(sql: str, dialect: str) -> dict[str, str]:
    """Source table -> the join that brings it in, e.g. "LEFT JOIN ON a.id = b.id"."""
    joins = {}
    for join in sqlglot.parse_one(sql, dialect=dialect).find_all(exp.Join):
        if not isinstance(join.this, exp.Table):  # subquery / derived table: no single name to key on
            continue
        kind = " ".join(filter(None, [join.side, join.kind, "JOIN"]))
        on = join.args.get("on")
        joins[exp.table_name(join.this)] = f"{kind} ON {on.sql(dialect=dialect)}" if on else kind
    return joins


def fetch_comments(tables: list[str], profile: str) -> dict[str, str]:
    """Unity Catalog comment per table. Any table that cannot be read is left out, not guessed."""
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient(profile=profile)
    comments = {}
    for table in tables:
        if table.count(".") != 2:  # not a three-level UC name — nothing to look up
            continue
        try:
            comment = client.tables.get(table).comment
        except Exception as err:
            print(f"note: no comment for {table} ({type(err).__name__})", file=sys.stderr)
            continue
        if comment:
            # UC comments are markdown; the diagram renders plain text, so unwrap [label](url)
            comments[table] = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", comment).strip()
    return comments


def build_graph(sql: str, dialect: str) -> tuple[dict[str, set[tuple[str, str]]], dict[str, list[str]]]:
    """(output column -> source columns, source table -> its columns), both in render order."""
    edges = {col: source_columns(sql, dialect, col) for col in output_columns(sql, dialect)}
    tables: dict[str, list[str]] = {}
    for table, column in sorted({src for sources in edges.values() for src in sources}):
        tables.setdefault(table, []).append(column)
    return edges, tables


def to_mermaid(
    edges: dict[str, set[tuple[str, str]]],
    tables: dict[str, list[str]],
    joins: dict[str, str] | None = None,
    comments: dict[str, str] | None = None,
) -> str:
    """Build a Mermaid flowchart of the query's column-level lineage."""
    joins, comments = joins or {}, comments or {}
    ids = {(t, c): f"s{i}" for i, (t, c) in enumerate((t, c) for t, cols in tables.items() for c in cols)}
    node = {table: re.sub(r"\W", "_", table) for table in tables}

    lines = ["flowchart LR"]
    for table, columns in tables.items():
        label = table
        if table in comments:
            label += f"<br/><i>{_mermaid_safe(_shorten(comments[table], 90))}</i>"
        # subgraph ids must be bare identifiers; the quoted label keeps the real name
        lines.append(f'  subgraph {node[table]}["{label}"]')
        lines += [f'    {ids[(table, column)]}["{column}"]' for column in columns]
        lines.append("  end")

    lines.append('  subgraph out["output"]')
    out_ids = {col: f"o{i}" for i, col in enumerate(edges)}
    lines += [f'    {out_ids[col]}["{col}"]' for col in edges]
    lines.append("  end")

    for col, sources in edges.items():
        lines += [f"  {ids[src]} --> {out_ids[col]}" for src in sorted(sources)]

    # The join is a relationship between tables, not between columns, so it is drawn as a dotted
    # edge between the subgraphs — visually distinct from the solid column-lineage arrows.
    driving = next((t for t in tables if t not in joins and t != UNQUALIFIED), None)
    for table, clause in joins.items():
        if table in node and driving:
            lines.append(f'  {node[driving]} -. "{_mermaid_safe(clause)}" .-> {node[table]}')
    return "\n".join(lines)


_SHAPES = {"SCAN": ("[(", ")]"), "JOIN": ("{{", "}}"), "OUTPUT": ("([", "])")}


def plan_to_mermaid(stages: list[Stage], comments: dict[str, str] | None = None) -> str:
    """Mermaid flowchart of the logical plan, one node per stage."""
    comments = comments or {}
    lines = ["flowchart LR"]
    for stage in stages:
        label = f"<b>{_mermaid_safe(stage.title)}</b>"
        note = next((c for t, c in comments.items() if stage.kind == "SCAN" and t in stage.title), None)
        if note:
            label += f"<br/><i>{_mermaid_safe(_shorten(note, 70))}</i>"
        for detail in stage.details:
            label += f"<br/>{_mermaid_safe(_shorten(detail, 70))}"
        open_, close = _SHAPES.get(stage.kind, ("[", "]"))
        lines.append(f'  {stage.id}{open_}"{label}"{close}')
    for stage in stages:
        lines += [f"  {dep} --> {stage.id}" for dep in stage.deps]
    return "\n".join(lines)


def plan_to_svg(stages: list[Stage], title: str, comments: dict[str, str] | None = None) -> str:
    """
    Top-down rendering: each row is one depth of the plan, so the query reads scans-first,
    output-last. Laid out vertically rather than left-to-right because a plan deep enough to be
    worth drawing (this one is 8 levels) runs to several thousand pixels wide on one line.
    """
    comments = comments or {}
    box_w, col_gap, row_gap, lead, head = 250, 26, 34, 13, 26

    depth: dict[str, int] = {}
    for stage in stages:  # stages are emitted dependency-first, so one pass suffices
        depth[stage.id] = max((depth[d] + 1 for d in stage.deps), default=0)

    text: dict[str, list[str]] = {}
    for stage in stages:
        note = next((c for t, c in comments.items() if stage.kind == "SCAN" and t in stage.title), None)
        body = _wrap(note, 44, 2) if note else []
        for detail in stage.details:
            body += _wrap(detail, 44, 2)
        text[stage.id] = body

    rows: dict[int, list[Stage]] = {}
    for stage in stages:
        rows.setdefault(depth[stage.id], []).append(stage)

    widest = max(len(row) for row in rows.values())
    width = 24 + widest * (box_w + col_gap) - col_gap + 24

    pos: dict[str, tuple[float, float]] = {}
    y = 60
    for level in sorted(rows):
        row = rows[level]
        span = len(row) * (box_w + col_gap) - col_gap
        x = (width - span) / 2  # centre each row, so the trunk of the plan stays vertical
        for stage in row:
            pos[stage.id] = (x, y)
            x += box_w + col_gap
        y += head + lead * max(len(text[s.id]) for s in row) + row_gap
    height = y - row_gap + 24

    curves, boxes = [], []
    for stage in stages:
        x, y = pos[stage.id]
        h = head + lead * len(text[stage.id])
        boxes.append(f'<rect x="{x}" y="{y}" width="{box_w}" height="{h}" class="{stage.kind.lower()}"/>')
        boxes.append(f'<text x="{x + 10}" y="{y + 17}" class="stt">{escape(stage.title)}</text>')
        for i, line in enumerate(text[stage.id]):
            boxes.append(f'<text x="{x + 10}" y="{y + head + lead * i + 3}" class="std">{escape(line)}</text>')
        for dep in stage.deps:
            dx, dy = pos[dep]
            a = dy + head + lead * len(text[dep])  # bottom edge of the dependency
            ax, bx = dx + box_w / 2, x + box_w / 2
            curves.append(f'<path d="M{ax},{a} C{ax},{a + 18} {bx},{y - 18} {bx},{y}" class="edge"/>')

    return "\n".join(
        [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
            f'viewBox="0 0 {width} {height}" font-family="{FONT}">',
            "<style>"
            ".bg{fill:#ffffff}.stt{fill:#1f2328;font-size:12px;font-weight:600}"
            ".std{fill:#57606a;font-size:10.5px}.ttl{fill:#1f2328;font-size:14px;font-weight:600}"
            # one class per box, never "st scan": multi-class cascade is not universally resolved
            ".scan{fill:#eef6ff;stroke:#54aeff;rx:5}.join{fill:#fbefff;stroke:#c297ff;rx:5}"
            ".filter{fill:#fff8e6;stroke:#e5b34a;rx:5}.aggregate{fill:#eafaef;stroke:#5bb974;rx:5}"
            ".sort{fill:#f6f8fa;stroke:#d0d7de;rx:5}.output{fill:#eceef1;stroke:#8c959f;rx:5}"
            ".step{fill:#f6f8fa;stroke:#d0d7de;rx:5}"
            ".edge{fill:none;stroke:#8c959f;stroke-width:1.3;opacity:.8}"
            "@media(prefers-color-scheme:dark){.bg{fill:#0d1117}.stt{fill:#e6edf3}.std{fill:#8b949e}"
            ".ttl{fill:#e6edf3}.scan{fill:#0d2847;stroke:#1f6feb}.join{fill:#2b1a3d;stroke:#8957e5}"
            ".filter{fill:#3a2d10;stroke:#9e6a03}.aggregate{fill:#0f2e1b;stroke:#2ea043}"
            ".sort{fill:#161b22;stroke:#30363d}.step{fill:#161b22;stroke:#30363d}"
            ".output{fill:#21262d;stroke:#6e7681}}"
            "</style>",
            f'<rect width="{width}" height="{height}" class="bg"/>',
            f'<text x="24" y="32" class="ttl">{escape(title)} — logical plan</text>',
            *curves,
            *boxes,
            "</svg>",
        ]
    )


def to_svg(
    edges: dict[str, set[tuple[str, str]]],
    tables: dict[str, list[str]],
    title: str,
    joins: dict[str, str] | None = None,
    comments: dict[str, str] | None = None,
) -> str:
    """
    Render the same graph as a standalone SVG.

    Written by hand rather than shelled out to `mmdc`, which would drag a Node toolchain and a
    headless Chromium into a Python repo — the same trade `scripts/star_history.py` makes. The
    layout is a fixed two-column bipartite one, which is all a lineage graph needs.
    """
    joins, comments = joins or {}, comments or {}
    row, box_h, left_w, right_w, gap = 26, 20, 340, 210, 230
    lead = 14  # line height for the wrapped annotation text
    width = 24 + left_w + gap + right_w + 24
    src_y, out_y = {}, {}

    y = 60
    rows = []
    for table, columns in tables.items():
        rows.append((y, f'<text x="24" y="{y + 14}" class="tbl">{escape(table)}</text>'))
        y += row
        for line in _wrap(comments.get(table, ""), 62, 2):
            rows.append((y, f'<text x="24" y="{y}" class="desc">{escape(line)}</text>'))
            y += lead
        # no join glyph: U+2A1D is missing from enough system fonts to render as tofu
        for line in _wrap(joins.get(table, ""), 58, 2):
            rows.append((y, f'<text x="24" y="{y}" class="join">{escape(line)}</text>'))
            y += lead
        if table in comments or table in joins:
            y += 6
        for column in columns:
            src_y[(table, column)] = y
            rows.append((y, f'<rect x="36" y="{y}" width="{left_w - 12}" height="{box_h}" class="node"/>'))
            rows.append((y, f'<text x="46" y="{y + 14}" class="col">{escape(column)}</text>'))
            y += row
        y += 8  # breathing room between tables

    oy = 60 + row  # align the first output box with the first source box
    for col in edges:
        out_y[col] = oy
        x = 24 + left_w + gap
        # single class, not "node out": some SVG renderers do not resolve a multi-class cascade
        rows.append((oy, f'<rect x="{x}" y="{oy}" width="{right_w}" height="{box_h}" class="out"/>'))
        rows.append((oy, f'<text x="{x + 10}" y="{oy + 14}" class="col">{escape(col)}</text>'))
        oy += row

    x1, x2 = 24 + left_w, 24 + left_w + gap
    curves = []
    for col, sources in edges.items():
        for src in sorted(sources):
            a, b = src_y[src] + box_h / 2, out_y[col] + box_h / 2
            curves.append(f'<path d="M{x1},{a} C{x1 + 90},{a} {x2 - 90},{b} {x2},{b}" class="edge"/>')

    height = max(y, oy) + 16
    return "\n".join(
        [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
            f'viewBox="0 0 {width} {height}" font-family="{FONT}">',
            # One file for both themes: GitHub renders SVGs through <img>, where the media query
            # still resolves against the reader's colour scheme.
            "<style>"
            ".bg{fill:#ffffff}.tbl{fill:#57606a;font-size:11px;font-weight:600;letter-spacing:.04em}"
            ".col{fill:#1f2328;font-size:12px}.node{fill:#f6f8fa;stroke:#d0d7de;rx:4}"
            ".desc{fill:#57606a;font-size:10.5px}.join{fill:#8250df;font-size:10.5px}"
            ".out{fill:#ddf4ff;stroke:#54aeff;rx:4}.edge{fill:none;stroke:#8c959f;stroke-width:1.2;opacity:.75}"
            ".ttl{fill:#1f2328;font-size:14px;font-weight:600}"
            "@media(prefers-color-scheme:dark){.bg{fill:#0d1117}.tbl{fill:#8b949e}.col{fill:#e6edf3}"
            ".desc{fill:#8b949e}.join{fill:#d2a8ff}"
            ".node{fill:#161b22;stroke:#30363d}.out{fill:#132e5c;stroke:#1f6feb}.ttl{fill:#e6edf3}}"
            "</style>",
            f'<rect width="{width}" height="{height}" class="bg"/>',
            f'<text x="24" y="32" class="ttl">{escape(title)}</text>',
            *curves,  # behind the boxes, so a curve never cuts across a label
            *[markup for _, markup in rows],
            "</svg>",
        ]
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--file", help="path to a .sql file (default: read from stdin)")
    parser.add_argument("--dialect", default="databricks", help="sqlglot dialect (default: databricks)")
    parser.add_argument("--name", help="output basename (default: the input filename, or 'query' on stdin)")
    parser.add_argument("--stdout", action="store_true", help="print the Mermaid source instead of writing files")
    parser.add_argument("--comments", action="store_true", help="annotate tables with their Unity Catalog comment")
    parser.add_argument("--profile", default="dev", help="Databricks profile for --comments (default: dev)")
    parser.add_argument(
        "--mode",
        choices=["plan", "lineage"],
        default="plan",
        help="plan: the query's steps (scans, each join, filter, aggregate, sort). "
        "lineage: which source column feeds each output column.",
    )
    args = parser.parse_args()

    sql = open(args.file).read() if args.file else sys.stdin.read()
    edges, tables = build_graph(sql, args.dialect)
    joins = extract_joins(sql, args.dialect)
    comments = fetch_comments(list(tables), args.profile) if args.comments else {}

    if args.mode == "plan":
        stages = build_plan(sql, args.dialect)
        mermaid = plan_to_mermaid(stages, comments)
    else:
        mermaid = to_mermaid(edges, tables, joins, comments)

    if args.stdout:
        print(mermaid)
        return

    name = args.name or (Path(args.file).stem if args.file else "query")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    # The query is written out verbatim alongside the diagram: a diagram without the SQL it came
    # from cannot be checked or regenerated, and the source is often an f-string in a .py file
    # that no longer reads the same once its placeholders are filled in.
    (OUT_DIR / f"{name}.sql").write_text(sql.strip() + "\n")
    (OUT_DIR / f"{name}.mmd").write_text(mermaid + "\n")
    svg = plan_to_svg(stages, name, comments) if args.mode == "plan" else to_svg(edges, tables, name, joins, comments)
    (OUT_DIR / f"{name}.svg").write_text(svg + "\n")
    print(f"Wrote {OUT_DIR / name}.sql, .mmd and .svg")


if __name__ == "__main__":
    main()
