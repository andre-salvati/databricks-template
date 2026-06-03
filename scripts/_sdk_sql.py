from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def get_warehouse_id(workspace: WorkspaceClient, name: str | None = None) -> str:
    """Resolve a SQL warehouse to use for DDL.

    Picking warehouses[0] is fragile — the API returns warehouses in an
    implementation-defined order, so on a workspace with multiple warehouses
    we'd silently grab whichever one happens to be first. Instead:

    - If `name` is provided, look it up by name (fail loudly if missing).
    - Otherwise, succeed only if there's exactly one warehouse; fail with
      a clear "ambiguous, pass --warehouse-name" error when there are many.
    """
    warehouses = list(workspace.warehouses.list())
    if not warehouses:
        raise ValueError("No SQL warehouse found. Please create one to run SQL statements.")

    if name:
        match = next((w for w in warehouses if w.name == name), None)
        if match is None:
            raise ValueError(f"SQL warehouse {name!r} not found. Available: {[w.name for w in warehouses]}")
        return match.id

    if len(warehouses) > 1:
        raise ValueError(
            f"Multiple SQL warehouses found ({[w.name for w in warehouses]}); pass --warehouse-name to disambiguate."
        )
    return warehouses[0].id


def run_sql(workspace: WorkspaceClient, warehouse_id: str, sql: str) -> None:
    print(f"  Running: {sql}")
    result = workspace.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
    )
    if result.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"Statement failed ({result.status.state}): {result.status.error}")
