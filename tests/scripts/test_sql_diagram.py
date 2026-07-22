"""
Unit tests for scripts/sql_diagram.py.

One test per defect found in the #50 review, so a regression names the thing it broke rather than
just failing. `scripts/` is not a package (the wheel ships `src/template` and `tests`), so the
module is loaded by path the same way a developer runs it.
"""

import importlib.util
import sys
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "sql_diagram", Path(__file__).resolve().parents[2] / "scripts" / "sql_diagram.py"
)
sql_diagram = importlib.util.module_from_spec(_SPEC)
sys.modules["sql_diagram"] = sql_diagram
_SPEC.loader.exec_module(sql_diagram)

DIALECT = "databricks"


def plan(sql: str):
    return sql_diagram.build_plan(sql_diagram.parse_select(sql, DIALECT), DIALECT)


def plan_text(sql: str) -> str:
    return sql_diagram.plan_to_mermaid(plan(sql))


def lineage_graph(sql: str):
    select = sql_diagram.parse_select(sql, DIALECT)
    return sql_diagram.build_graph(sql, select, DIALECT)


# --- finding 1: plan mode inherited lineage's SELECT * restriction ------------------------------


def test_plan_mode_handles_select_star():
    """SELECT * has no per-column lineage, but its plan is perfectly drawable."""
    stages = plan("SELECT * FROM main.raw.orders")
    assert [s.kind for s in stages] == ["SCAN", "OUTPUT"]
    assert stages[0].source == "main.raw.orders"


def test_lineage_mode_still_rejects_select_star():
    with pytest.raises(SystemExit, match="SELECT \\*"):
        lineage_graph("SELECT * FROM main.raw.orders")


# --- finding 2: a global TRUE AND regex deleted legitimate predicate text ------------------------


def test_true_and_inside_a_predicate_survives():
    """sqlglot pads a join's residual predicate with TRUE AND; only the padding may be dropped."""
    detail = "\n".join(
        d
        for s in plan(
            "SELECT a.id FROM main.raw.a a JOIN main.raw.b b ON a.id = b.id AND (b.flag = TRUE AND b.x > 1 OR b.y < 2)"
        )
        if s.kind == "JOIN"
        for d in s.details
    )
    assert "b.flag = TRUE" in detail, detail
    assert "b.x > 1" in detail, detail


def test_join_padding_is_not_shown():
    joins = [s for s in plan("SELECT a.id FROM main.raw.a a JOIN main.raw.b b ON a.id = b.id") if s.kind == "JOIN"]
    assert [d for s in joins for d in s.details] == ["a.id = b.id"]


# --- finding 3: conditions were split on generated text, not the AST ----------------------------


def test_and_inside_a_string_literal_is_not_split():
    parts = sql_diagram._and_parts("a.note = 'x AND y' AND a.z > 1", DIALECT)
    assert parts == ["a.note = 'x AND y'", "a.z > 1"]


def test_paren_inside_a_string_literal_does_not_break_splitting():
    parts = sql_diagram._and_parts("a.q = 'left ) paren' AND a.z > 1 AND a.w < 2", DIALECT)
    assert parts == ["a.q = 'left ) paren'", "a.z > 1", "a.w < 2"]


def test_nested_conjunction_inside_parens_stays_whole():
    """The grouped predicate stays on one line; only top-level ANDs split."""
    parts = sql_diagram._and_parts("a.x = 1 AND (b.y = 2 AND b.z = 3 OR b.w = 4)", DIALECT)
    assert parts[0] == "a.x = 1"
    assert len(parts) == 2
    assert "OR b.w = 4" in parts[1] and "b.y = 2 AND b.z = 3" in parts[1]


def test_an_or_conjunct_keeps_its_parentheses():
    """Lines are joined by "and"; an unparenthesised OR would read as the opposite grouping."""
    parts = sql_diagram._and_parts("a.x = 1 AND (b.y IS NULL OR b.z < 2)", DIALECT)
    assert parts[1] == "(b.y IS NULL OR b.z < 2)"


# --- finding 4: duplicate output column names collapsed, dropping a source table -----------------


def test_duplicate_output_names_are_refused_not_silently_collapsed():
    """
    sqlglot resolves lineage by column name, so `SELECT a.id, b.id` traces both to a.id. Keying
    the graph by index would keep both nodes but attribute one to the wrong table; refusing is the
    only answer that cannot mislead.
    """
    with pytest.raises(SystemExit, match="more than once"):
        lineage_graph("SELECT a.id, b.id, a.amt FROM main.raw.a a JOIN main.raw.b b ON a.id = b.id")


def test_duplicate_output_names_do_not_affect_plan_mode():
    sql = "SELECT a.id, b.id FROM main.raw.a a JOIN main.raw.b b ON a.id = b.id"
    assert [s.kind for s in plan(sql)] == ["SCAN", "SCAN", "JOIN", "OUTPUT"]


def test_distinct_output_names_keep_select_order():
    edges, tables = lineage_graph("SELECT a.id, b.id AS b_id, a.amt FROM main.raw.a a JOIN main.raw.b b ON a.id = b.id")
    assert [name for name, _ in edges] == ["id", "b_id", "amt"]
    assert "main.raw.a" in tables and "main.raw.b" in tables


# --- finding 5: unsupported statement shapes raised raw tracebacks ------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TABLE main.raw.t AS SELECT a.id FROM main.raw.a a",
        "INSERT INTO main.raw.t SELECT a.id FROM main.raw.a a",
    ],
)
def test_wrapped_selects_are_unwrapped_not_rejected(sql):
    assert [s.kind for s in plan(sql)] == ["SCAN", "OUTPUT"]


@pytest.mark.parametrize("sql", ["", "   \n  ", "SELECT FROM WHERE ("])
def test_unusable_input_exits_cleanly(sql):
    with pytest.raises(SystemExit) as excinfo:
        plan(sql)
    assert "sql-diagram:" in str(excinfo.value)


def test_statement_without_a_select_exits_cleanly():
    with pytest.raises(SystemExit, match="no SELECT"):
        plan("DELETE FROM main.raw.a WHERE id = 1")


# --- finding 6: a cross product was labelled INNER ----------------------------------------------


def test_cross_join_is_not_labelled_inner():
    titles = [s.title for s in plan("SELECT o.id, c.name FROM main.raw.orders o CROSS JOIN main.raw.cust c")]
    assert any("CROSS" in t for t in titles), titles
    assert not any("INNER" in t for t in titles), titles


def test_inner_join_with_keys_is_still_labelled_inner():
    titles = [s.title for s in plan("SELECT a.id FROM main.raw.a a JOIN main.raw.b b ON a.id = b.id")]
    assert any("INNER" in t for t in titles), titles


# --- finding 7: comments were matched to scans by substring -------------------------------------


def test_comment_goes_to_the_exact_table_not_a_name_prefix():
    stages = plan("SELECT u.id, e.id FROM main.raw.usage u JOIN main.raw.usage_extra e ON u.id = e.id")
    text = sql_diagram.plan_to_mermaid(
        stages, {"main.raw.usage": "COMMENT-FOR-USAGE", "main.raw.usage_extra": "COMMENT-FOR-EXTRA"}
    )
    for line in text.splitlines():
        if "SCAN main.raw.usage_extra" in line:
            assert "COMMENT-FOR-EXTRA" in line and "COMMENT-FOR-USAGE" not in line, line
            break
    else:
        pytest.fail(f"no scan node for usage_extra:\n{text}")


# --- finding 8: join edges vanished when the driving table projected nothing ---------------------


def test_join_edge_survives_a_driving_table_projecting_no_columns():
    sql = "SELECT b.x, b.y FROM main.raw.a a JOIN main.raw.b b ON a.id = b.id"
    select = sql_diagram.parse_select(sql, DIALECT)
    edges, tables = sql_diagram.build_graph(sql, select, DIALECT)
    text = sql_diagram.to_mermaid(
        edges,
        tables,
        sql_diagram.extract_joins(select, DIALECT),
        {},
        sql_diagram.driving_table(select, DIALECT),
    )
    assert "-." in text, f"join edge dropped:\n{text}"
    assert "main.raw.a" in text


def test_driving_table_comes_from_the_from_clause():
    select = sql_diagram.parse_select("SELECT b.x FROM main.raw.a a JOIN main.raw.b b ON a.id = b.id", DIALECT)
    assert sql_diagram.driving_table(select, DIALECT) == "main.raw.a"


# --- determinism (guards the fix that shipped in this branch) -----------------------------------


def test_plan_output_is_stable_across_runs():
    sql = (
        "WITH n AS (SELECT id, MAX(x) AS x FROM main.raw.c GROUP BY id) "
        "SELECT a.id, SUM(b.v) AS v FROM main.raw.a a "
        "LEFT JOIN main.raw.b b ON a.id = b.id LEFT JOIN n ON n.id = a.id "
        "WHERE a.d > 1 GROUP BY a.id ORDER BY v DESC"
    )
    assert len({plan_text(sql) for _ in range(5)}) == 1


def test_every_join_gets_its_own_numbered_stage():
    sql = "SELECT a.id FROM main.raw.a a LEFT JOIN main.raw.b b ON a.id = b.id LEFT JOIN main.raw.c c ON a.id = c.id"
    titles = [s.title for s in plan(sql) if s.kind == "JOIN"]
    assert len(titles) == 2
    assert titles[0].startswith("JOIN 1") and titles[1].startswith("JOIN 2")
