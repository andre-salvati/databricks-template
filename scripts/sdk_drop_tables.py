import argparse
import re

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import TableType

from _sdk_sql import get_warehouse_id, run_sql
from template.config import MEDALLION_SCHEMAS

# DROP must match the object kind: a materialized view / view cannot be removed with
# DROP TABLE (it raises a type-mismatch error that IF EXISTS does NOT suppress). The SDP
# pipeline materializes MVs and streaming tables in the raw/curated/report schemas, so a
# blind DROP TABLE loop aborts mid-way and leaves the catalog half-dropped.
#
# Some SQL warehouse channels also reject the `DROP STREAMING TABLE` grammar with a
# PARSE_SYNTAX_ERROR (while accepting a plain DROP TABLE on the streaming table). So we try
# the kind-specific statement first and fall back to DROP TABLE — _drop_candidates() always
# appends DROP TABLE as the last resort.
_DROP_STMT = {
    TableType.MATERIALIZED_VIEW: "DROP MATERIALIZED VIEW IF EXISTS",
    TableType.STREAMING_TABLE: "DROP STREAMING TABLE IF EXISTS",
    TableType.VIEW: "DROP VIEW IF EXISTS",
    TableType.METRIC_VIEW: "DROP VIEW IF EXISTS",
}


def _drop_candidates(table_type) -> list[str]:
    """DROP statements to try in order. The kind-specific form first (when there is one),
    then plain DROP TABLE as a fallback for warehouses whose parser rejects the
    kind-specific grammar (notably DROP STREAMING TABLE)."""
    specific = _DROP_STMT.get(table_type)
    candidates = [specific] if specific else []
    if "DROP TABLE IF EXISTS" not in candidates:
        candidates.append("DROP TABLE IF EXISTS")
    return candidates


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
            fq = f"`{catalog}`.`{schema}`.`{table.name}`"
            last_err: Exception | None = None
            for stmt in _drop_candidates(table.table_type):
                try:
                    run_sql(workspace, warehouse_id, f"{stmt} {fq}")
                    last_err = None
                    break
                except Exception as e:  # try the next candidate (e.g. parser rejects DROP STREAMING TABLE)
                    last_err = e
            if last_err is not None:
                raise last_err
            count += 1

    print(f"Done. {count} object(s) dropped.")


if __name__ == "__main__":
    main()
