"""
Show AWS and Databricks spend for the last 30 days, day by day.
Usage: uv run python scripts/project_costs.py [--days N] [--profile PROFILE] [--aws-profile PROFILE]
"""

import argparse
import json
import subprocess
import time
from datetime import date, timedelta

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

_POLL_TERMINAL = {StatementState.SUCCEEDED, StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED}


def aws_daily_costs(days: int, profile: str | None = None) -> None:
    end = date.today()
    start = end - timedelta(days=days)

    cmd = [
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
    ]
    if profile:
        cmd += ["--profile", profile]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError:
        print("AWS error: AWS CLI not found — install it or ensure it is on PATH.\n")
        return

    if result.returncode != 0:
        # Don't echo raw stderr — AWS error messages can include caller ARNs with account IDs.
        print("AWS error: unable to retrieve cost data (check AWS credentials and ce:GetCostAndUsage permission).\n")
        return

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        print("AWS error: unexpected response from AWS CLI (pager or credential helper output?).\n")
        return

    # Raw data as a DataFrame (one row per day/service), printed before the formatted report.
    aws_df = pd.DataFrame(
        [
            {
                "date": period["TimePeriod"]["Start"],
                "service": group["Keys"][0],
                "blended_cost_usd": float(group["Metrics"]["BlendedCost"]["Amount"]),
                "estimated": bool(period.get("Estimated")),
            }
            for period in data["ResultsByTime"]
            for group in period["Groups"]
        ],
        columns=["date", "service", "blended_cost_usd", "estimated"],
    )
    print(f"\nAWS raw data — last {days} days (DataFrame, {len(aws_df)} rows)")
    print(aws_df.to_string(index=False))

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

    # Aggregate by service type over the whole window.
    by_service = aws_df.groupby("service")["blended_cost_usd"].sum().sort_values(ascending=False)
    print(f"AWS Costs by Service — last {days} days")
    print(f"{'Service':<40} {'Total USD':>12}")
    print("-" * 54)
    for svc, amt in by_service.items():
        if amt > 0:
            print(f"{svc:<40.40} {amt:>12.4f}")
    print("-" * 54)
    print(f"{'Total':<40} {by_service.sum():>12.4f}")
    # The daily table marks estimated days with '*'; the rollup sums estimated + finalized
    # cost into one total, so flag it rather than let the reader assume these are final.
    if bool(aws_df["estimated"].any()):
        print("Totals include estimated (not-yet-finalized) costs.")
    print()


def databricks_daily_costs(profile: str, days: int) -> None:
    print(f"Databricks Daily Costs — last {days} days")

    try:
        w = WorkspaceClient(profile=profile)
    except Exception as e:
        # Avoid printing the exception directly — SDK exceptions can include the workspace URL.
        print(f"Could not connect to Databricks ({profile} profile): {type(e).__name__}\n")
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
        while stmt.status.state not in _POLL_TERMINAL:
            time.sleep(1)
            stmt = w.statement_execution.get_statement(stmt.statement_id)

        if stmt.status.state != StatementState.SUCCEEDED:
            error_msg = stmt.status.error.message if stmt.status.error else stmt.status.state.value
            raise RuntimeError(error_msg)

        rows = stmt.result.data_array or []
        if not rows:
            print("No usage data found.\n")
            return

        # Raw data as a DataFrame (quantities arrive as strings from the SQL result),
        # printed before the formatted report.
        usage_df = pd.DataFrame(rows, columns=["usage_date", "sku_name", "total_quantity", "usage_unit"])
        print(f"\nDatabricks raw data — last {days} days (DataFrame, {len(usage_df)} rows)")
        print(usage_df.to_string(index=False))
        print()

        # Convert quantity once, then drive both the daily table and the rollup off the frame.
        usage_df["total_quantity"] = usage_df["total_quantity"].astype(float)

        print(f"{'Date':<12} {'SKU':<55} {'Quantity':>10}  Unit")
        print("-" * 85)
        for r in usage_df.itertuples(index=False):
            print(f"{str(r.usage_date):<12} {str(r.sku_name):<55.55} {r.total_quantity:>10.4f}  {r.usage_unit}")
        print()

        # Aggregate by SKU (service type) over the whole window. Quantities stay grouped by
        # unit since DBU / DSU / GB are not summable across SKUs; dropna=False keeps a SKU with
        # a NULL unit visible (pandas would otherwise drop NaN group keys and the rollup would
        # silently disagree with the daily table). Sort within unit so the ordering is meaningful.
        by_sku = (
            usage_df.groupby(["sku_name", "usage_unit"], as_index=False, dropna=False)["total_quantity"]
            .sum()
            .sort_values(["usage_unit", "total_quantity"], ascending=[True, False])
        )
        print(f"Databricks Costs by SKU — last {days} days")
        print(f"{'SKU':<55} {'Quantity':>12}  Unit")
        print("-" * 75)
        for r in by_sku.itertuples(index=False):
            print(f"{r.sku_name:<55.55} {r.total_quantity:>12.4f}  {r.usage_unit}")
        print()

    except Exception as e:
        print(f"system.billing.usage not available: {e}")
        print("Free-tier workspaces don't expose system.billing (requires Premium).\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Show AWS and Databricks spend day by day")
    parser.add_argument("--days", type=int, default=30, help="Number of days to look back (default: 30)")
    parser.add_argument("--profile", default="dev", help="Databricks CLI profile (default: dev)")
    parser.add_argument(
        "--aws-profile",
        default=None,
        help="AWS CLI profile for Cost Explorer (default: AWS default credential chain)",
    )
    args = parser.parse_args()

    if args.days < 1:
        parser.error("--days must be a positive integer")

    aws_daily_costs(args.days, args.aws_profile)
    databricks_daily_costs(args.profile, args.days)


if __name__ == "__main__":
    main()
