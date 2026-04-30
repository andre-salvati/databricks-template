import argparse

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def find_existing(workspace: WorkspaceClient, display_name: str):
    for sp in workspace.service_principals.list():
        if sp.display_name == display_name:
            return sp
    return None


def get_warehouse_id(workspace: WorkspaceClient) -> str:
    """Get the first available SQL warehouse ID."""
    warehouses = list(workspace.warehouses.list())
    if not warehouses:
        raise ValueError("No SQL warehouse found. Please create one to query system tables.")
    return warehouses[0].id


def grant_permissions(workspace: WorkspaceClient, display_name: str):
    current_user = workspace.current_user.me().user_name
    statements = [
        f"GRANT CREATE CATALOG ON Metastore TO `e9839ada-817e-4fc7-abb1-2554cce3fcd8`",
        f"GRANT BROWSE ON CATALOG staging TO `e9839ada-817e-4fc7-abb1-2554cce3fcd8`",
        f"GRANT USE CATALOG ON CATALOG staging TO `e9839ada-817e-4fc7-abb1-2554cce3fcd8`",
        f"GRANT CREATE SCHEMA ON CATALOG staging TO `e9839ada-817e-4fc7-abb1-2554cce3fcd8`",
        f"GRANT USE SCHEMA ON SCHEMA staging.system TO `e9839ada-817e-4fc7-abb1-2554cce3fcd8`",
        f"GRANT SELECT ON SCHEMA staging.system TO `e9839ada-817e-4fc7-abb1-2554cce3fcd8`",
        f"GRANT USE SCHEMA ON SCHEMA staging.raw TO `e9839ada-817e-4fc7-abb1-2554cce3fcd8`",
        f"GRANT MANAGE ON SCHEMA staging.raw TO `e9839ada-817e-4fc7-abb1-2554cce3fcd8`",
        f"GRANT USE SCHEMA ON SCHEMA staging.curated TO `e9839ada-817e-4fc7-abb1-2554cce3fcd8`",
        f"GRANT MANAGE ON SCHEMA staging.curated TO `e9839ada-817e-4fc7-abb1-2554cce3fcd8`",
        f"GRANT USE SCHEMA ON SCHEMA staging.refined TO `e9839ada-817e-4fc7-abb1-2554cce3fcd8`",
        f"GRANT MANAGE ON SCHEMA staging.refined TO `e9839ada-817e-4fc7-abb1-2554cce3fcd8`",
        f"GRANT BROWSE ON CATALOG prod TO `e9839ada-817e-4fc7-abb1-2554cce3fcd8`",
        f"GRANT USE CATALOG ON CATALOG prod TO `e9839ada-817e-4fc7-abb1-2554cce3fcd8`",
        f"GRANT USE SCHEMA ON SCHEMA prod.system TO `e9839ada-817e-4fc7-abb1-2554cce3fcd8`",
        f"GRANT CREATE TABLE ON SCHEMA prod.system TO `e9839ada-817e-4fc7-abb1-2554cce3fcd8`",
        f"GRANT SELECT ON SCHEMA prod.system TO `e9839ada-817e-4fc7-abb1-2554cce3fcd8`",
    ]
    warehouse_id = get_warehouse_id(workspace)
    for sql in statements:
        print(f"  Running: {sql}")
        result = workspace.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="50s",
        )
        if result.status.state != StatementState.SUCCEEDED:
            raise RuntimeError(f"Statement failed ({result.status.state}): {result.status.error}")


def main():
    parser = argparse.ArgumentParser(description="Create a Databricks service principal and print its application ID.")
    parser.add_argument(
        "display_name",
        help="Display name for the service principal",
    )
    args = parser.parse_args()

    workspace = WorkspaceClient(profile="dev")  # as configured in .databrickscfg

    existing = find_existing(workspace, args.display_name)
    if existing:
        print(f"Service principal '{existing.display_name}' already exists (applicationId={existing.application_id}).")
    else:
        print(f"Creating service principal '{args.display_name}'...")
        sp = workspace.service_principals.create(display_name=args.display_name)
        print(f"\nService principal created successfully.")
        print(f"  Display name  : {sp.display_name}")
        print(f"  Application ID: {sp.application_id}")

    print("\nGranting permissions...")
    grant_permissions(workspace, args.display_name)
    print("Done.")


if __name__ == "__main__":
    main()
