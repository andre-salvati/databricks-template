import argparse
import re

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import TableType

from _sdk_sql import get_warehouse_id, run_sql
from template.config import MEDALLION_SCHEMAS

_TRUNCATABLE = {TableType.MANAGED, TableType.EXTERNAL}


def _resolve_catalog(workspace: WorkspaceClient, env: str) -> str:
    if env == "dev":
        local_part = workspace.current_user.me().user_name.split("@")[0]
        user = re.sub(r"[^a-zA-Z0-9_]", "_", local_part)
        return f"dev_{user}"
    return env


def main():
    parser = argparse.ArgumentParser(description="Truncate all medallion tables in a target environment.")
    parser.add_argument("env", choices=["dev", "staging", "prod"])
    parser.add_argument("--yes", "-y", action="store_true", help="Confirm truncation (required for staging/prod).")
    parser.add_argument(
        "--warehouse-name",
        default=None,
        help="SQL warehouse name to use. Required when the workspace has multiple warehouses.",
    )
    args = parser.parse_args()

    if args.env in ("staging", "prod") and not args.yes:
        print(f"This will truncate ALL tables in '{args.env}'. Pass --yes to confirm.")
        raise SystemExit(1)

    workspace = WorkspaceClient(profile=args.env)
    warehouse_id = get_warehouse_id(workspace, args.warehouse_name)
    catalog = _resolve_catalog(workspace, args.env)

    print(f"Truncating all tables in catalog '{catalog}'...")
    count = 0
    for schema in MEDALLION_SCHEMAS:
        for table in workspace.tables.list(catalog_name=catalog, schema_name=schema):
            if table.table_type not in _TRUNCATABLE:
                print(f"  Skipping `{catalog}`.`{schema}`.`{table.name}` ({table.table_type})")
                continue
            run_sql(workspace, warehouse_id, f"TRUNCATE TABLE `{catalog}`.`{schema}`.`{table.name}`")
            count += 1

    print(f"Done. {count} table(s) truncated.")


if __name__ == "__main__":
    main()
