import argparse

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
from databricks.sdk.service.sql import StatementState


SP_DISPLAY_NAME = "template-sp"
# Per-developer dev catalogs (`dev_<short_name>`) and their schemas are created
# on the fly by Config when a developer runs the wheel — see src/template/config.py.
# Only the shared `staging` and `prod` catalogs are bootstrapped here.
CATALOGS = ["staging", "prod"]
# Keep aligned with template.config.MEDALLION_SCHEMAS so staging/prod come up with
# the same schema layout that dev creates on the fly.
SCHEMAS = ["external_source", "raw", "curated", "report", "ops"]


def _get_warehouse_id(workspace: WorkspaceClient) -> str:
    warehouses = list(workspace.warehouses.list())
    if not warehouses:
        raise ValueError("No SQL warehouse found. Please create one to run SQL statements.")
    return warehouses[0].id


def _run_sql(workspace: WorkspaceClient, warehouse_id: str, sql: str):
    print(f"  Running: {sql}")
    result = workspace.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
    )
    if result.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"Statement failed ({result.status.state}): {result.status.error}")


# --- Service principal ---


def _find_sp(workspace: WorkspaceClient, display_name: str):
    for sp in workspace.service_principals.list():
        if sp.display_name == display_name:
            return sp
    return None


def create_service_principal(workspace: WorkspaceClient, display_name: str) -> str:
    existing = _find_sp(workspace, display_name)
    if existing:
        print(f"Service principal '{existing.display_name}' already exists.")
        sp_numeric_id = existing.id
        sp_id = str(existing.application_id)
    else:
        print(f"Creating service principal '{display_name}'...")
        sp = workspace.service_principals.create(display_name=display_name)
        print(f"  Created '{sp.display_name}'.")
        sp_numeric_id = sp.id
        sp_id = str(sp.application_id)

    print(f"  Application ID: {sp_id}")

    current_user = workspace.current_user.me().user_name
    print(f"  SCIM ID: {sp_numeric_id}")
    try:
        workspace.permissions.set(
            request_object_type="service-principals",
            request_object_id=str(sp_numeric_id),
            access_control_list=[
                AccessControlRequest(user_name=current_user, permission_level=PermissionLevel.CAN_USE)
            ],
        )
        print(f"  Granted CAN_USE on service principal to '{current_user}'.")
    except Exception as e:
        print(f"  Warning: could not set CAN_USE on service principal ({e}). Continuing.")

    warehouse_id = _get_warehouse_id(workspace)
    _run_sql(workspace, warehouse_id, f"GRANT CREATE CATALOG ON Metastore TO `{sp_id}`")

    print(f"\nSP_ID={sp_id}")
    return sp_id


# --- Catalogs and schemas ---


def _existing_catalogs(workspace: WorkspaceClient) -> set[str]:
    return {c.name for c in workspace.catalogs.list()}


def _existing_schemas(workspace: WorkspaceClient, catalog: str) -> set[str]:
    return {s.name for s in workspace.schemas.list(catalog_name=catalog)}


def create_catalogs_and_schemas(workspace: WorkspaceClient, sp_id: str, storage_root: str | None):
    warehouse_id = _get_warehouse_id(workspace)
    existing_cats = _existing_catalogs(workspace)

    for catalog in CATALOGS:
        print(f"\nProcessing catalog '{catalog}'...")
        if catalog in existing_cats:
            print(f"  Catalog '{catalog}' already exists, skipping.")
        else:
            kwargs = {"name": catalog}
            if storage_root:
                kwargs["storage_root"] = storage_root.rstrip("/") + f"/{catalog}"
            workspace.catalogs.create(**kwargs)
            print(f"  Created catalog '{catalog}'.")

        _run_sql(workspace, warehouse_id, f"GRANT ALL PRIVILEGES ON CATALOG `{catalog}` TO `{sp_id}`")

        existing_schs = _existing_schemas(workspace, catalog)
        for schema in SCHEMAS:
            if schema in existing_schs:
                print(f"    Schema '{catalog}.{schema}' already exists, skipping.")
            else:
                workspace.schemas.create(name=schema, catalog_name=catalog)
                print(f"    Created schema '{catalog}.{schema}'.")
            _run_sql(workspace, warehouse_id, f"GRANT MANAGE ON SCHEMA `{catalog}`.`{schema}` TO `{sp_id}`")


# --- Entry point ---


def main():
    parser = argparse.ArgumentParser(
        description="Bootstrap a Databricks workspace: create service principal, catalogs, and schemas."
    )
    parser.add_argument(
        "--sp-name",
        default=SP_DISPLAY_NAME,
        help=f"Service principal display name (default: {SP_DISPLAY_NAME})",
    )
    parser.add_argument(
        "--storage-root",
        default=None,
        help="S3/ADLS base path for managed storage (e.g. s3://my-bucket). Catalog name is appended automatically.",
    )
    parser.add_argument(
        "--profile",
        default="dev",
        help="Databricks CLI profile to use (default: dev)",
    )
    args = parser.parse_args()

    workspace = WorkspaceClient(profile=args.profile)

    print("=== Step 1: Service principal ===")
    sp_id = create_service_principal(workspace, args.sp_name)

    print("\n=== Step 2: Catalogs and schemas ===")
    create_catalogs_and_schemas(workspace, sp_id, args.storage_root)

    print("\nDone.")


if __name__ == "__main__":
    main()
