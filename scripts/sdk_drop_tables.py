import argparse
import re

from databricks.sdk import WorkspaceClient

from _sdk_sql import get_warehouse_id, run_sql
from template.config import MEDALLION_SCHEMAS


def _resolve_catalog(workspace: WorkspaceClient, env: str) -> str:
    if env == "dev":
        local_part = workspace.current_user.me().user_name.split("@")[0]
        user = re.sub(r"[^a-zA-Z0-9_]", "_", local_part)
        return f"dev_{user}"
    return env


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Drop ALL medallion tables in a target environment. Unlike TRUNCATE, this "
            "removes the table definitions too, so the next pipeline run recreates them "
            "with the current schema — the supported way to apply a schema migration."
        )
    )
    parser.add_argument("env", choices=["dev", "staging", "prod"])
    parser.add_argument("--yes", "-y", action="store_true", help="Confirm drop (required for staging/prod).")
    parser.add_argument(
        "--warehouse-name",
        default=None,
        help="SQL warehouse name to use. Required when the workspace has multiple warehouses.",
    )
    args = parser.parse_args()

    if args.env in ("staging", "prod") and not args.yes:
        print(f"This will DROP ALL tables in '{args.env}'. Pass --yes to confirm.")
        raise SystemExit(1)

    workspace = WorkspaceClient(profile=args.env)
    warehouse_id = get_warehouse_id(workspace, args.warehouse_name)
    catalog = _resolve_catalog(workspace, args.env)

    print(f"Dropping all tables in catalog '{catalog}'...")
    count = 0
    for schema in MEDALLION_SCHEMAS:
        for table in workspace.tables.list(catalog_name=catalog, schema_name=schema):
            run_sql(workspace, warehouse_id, f"DROP TABLE IF EXISTS `{catalog}`.`{schema}`.`{table.name}`")
            count += 1

    print(f"Done. {count} table(s) dropped.")


if __name__ == "__main__":
    main()
