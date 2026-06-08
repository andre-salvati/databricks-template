"""
Show AWS and Databricks spend for the last 30 days, day by day.
Usage: uv run python scripts/project_costs.py [--days N] [--profile PROFILE]
"""

import argparse
import json
import subprocess
import time
from datetime import date, timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def aws_daily_costs(days: int) -> None:
    end = date.today()
    start = end - timedelta(days=days)

    result = subprocess.run(
        [
            "aws",
            "ce",
            "get-cost-and-usage",
            "--time-period",
            f"Start={start},End={end}",
            "--granularity",
            "DAILY",
            "--metrics",
            "BlendedCost",
            "--group-by",
            "Type=DIMENSION,Key=SERVICE",
            "--output",
            "json",
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"AWS error: {result.stderr.strip()}")
        return

    data = json.loads(result.stdout)

    print(f"\nAWS Daily Costs — last {days} days")
    print(f"{'Date':<12} {'Total USD':>10}  Services")
    print("-" * 80)

    grand_total = 0.0
    for period in data["ResultsByTime"]:
        day = period["TimePeriod"]["Start"]
        estimated = "*" if period.get("Estimated") else " "
        groups = [(g["Keys"][0], float(g["Metrics"]["BlendedCost"]["Amount"])) for g in period["Groups"]]
        total = sum(amt for _, amt in groups)
        grand_total += total
        services = ", ".join(
            f"{svc.split()[-1]} ${amt:.4f}" for svc, amt in sorted(groups, key=lambda x: -x[1]) if amt > 0
        )
        print(f"{day}{estimated} {total:>10.4f}  {services or '—'}")

    print("-" * 80)
    print(f"{'Total':<13} {grand_total:>10.4f}")
    print("* = estimated\n")


def databricks_daily_costs(profile: str, days: int) -> None:
    print(f"Databricks Daily Costs — last {days} days")

    try:
        w = WorkspaceClient(profile=profile)
    except Exception as e:
        print(f"Could not connect to Databricks ({profile} profile): {e}\n")
        return

    warehouses = list(w.warehouses.list())
    if not warehouses:
        print("No SQL warehouse available.\n")
        return
    warehouse_id = warehouses[0].id

    sql = f"""
    SELECT
      usage_date,
      sku_name,
      ROUND(SUM(usage_quantity), 4) AS total_quantity,
      usage_unit
    FROM system.billing.usage
    WHERE usage_date >= CURRENT_DATE() - INTERVAL {days} DAYS
    GROUP BY usage_date, sku_name, usage_unit
    ORDER BY usage_date DESC, total_quantity DESC
    """

    try:
        stmt = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="30s",
        )
        while stmt.status.state not in [
            StatementState.SUCCEEDED,
            StatementState.FAILED,
            StatementState.CANCELED,
        ]:
            time.sleep(1)
            stmt = w.statement_execution.get_statement(stmt.statement_id)

        if stmt.status.state != StatementState.SUCCEEDED:
            raise RuntimeError(stmt.status.error.message)

        rows = stmt.result.data_array or []
        if not rows:
            print("No usage data found.\n")
            return

        print(f"{'Date':<12} {'SKU':<55} {'Quantity':>10}  Unit")
        print("-" * 85)
        for row in rows:
            print(f"{str(row[0]):<12} {str(row[1]):<55.55} {float(row[2]):>10.4f}  {row[3]}")
        print()

    except Exception as e:
        print(f"system.billing.usage not available: {e}")
        print("Free-tier workspaces don't expose system.billing (requires Premium).\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Show AWS and Databricks spend day by day")
    parser.add_argument("--days", type=int, default=30, help="Number of days to look back (default: 30)")
    parser.add_argument("--profile", default="dev", help="Databricks CLI profile (default: dev)")
    args = parser.parse_args()

    aws_daily_costs(args.days)
    databricks_daily_costs(args.profile, args.days)


if __name__ == "__main__":
    main()
