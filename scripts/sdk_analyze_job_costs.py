"""
Analyze costs of Databricks jobs run today.

This script queries Databricks system tables to calculate costs for jobs
executed today, providing insights into compute usage and billing.

Usage:
    python scripts/analyze_job_costs.py [--profile PROFILE] [--date DATE]

Examples:
    python scripts/analyze_job_costs.py
    python scripts/analyze_job_costs.py --profile dev
    python scripts/analyze_job_costs.py --date 2024-01-15
"""

import argparse
from datetime import datetime, timedelta
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
import time


def get_todays_date():
    """Get today's date in YYYY-MM-DD format."""
    return datetime.now().strftime("%Y-%m-%d")


def analyze_job_costs(workspace: WorkspaceClient, target_date: str):
    """
    Analyze job costs for a specific date using system tables.

    Args:
        workspace: Databricks WorkspaceClient instance
        target_date: Date to analyze in YYYY-MM-DD format
    """
    print(f"\n{'=' * 80}")
    print(f"Job Cost Analysis for {target_date}")
    print(f"{'=' * 80}\n")

    # Query to get usage data from system tables
    # Note: This assumes system tables are enabled for the metastore
    query = f"""
    SELECT
        u.usage_metadata.job_id,
        u.usage_metadata.job_name,
        u.sku_name,
        u.cloud,
        SUM(usage_quantity) as total_dbu,
        COUNT(DISTINCT u.usage_metadata.job_run_id) as num_runs,
        ROUND(SUM(usage_quantity * list_prices.pricing.default), 2) as estimated_cost_usd
    FROM
        system.billing.usage u
    LEFT JOIN
        system.billing.list_prices ON u.sku_name = list_prices.sku_name
        AND u.cloud = list_prices.cloud
    WHERE
        usage_date = '{target_date}'
        AND 
        u.usage_metadata.job_id IS NOT NULL
    GROUP BY
        u.usage_metadata.job_id,
        u.usage_metadata.job_name,
        u.sku_name,
        u.cloud
    ORDER BY
        estimated_cost_usd DESC
    """

    try:
        print("Querying system.billing.usage table...\n")
        result = workspace.statement_execution.execute_statement(
            warehouse_id=get_warehouse_id(workspace),
            statement=query,
            wait_timeout="0s",  # async
        )

        # Poll for result completion
        while True:
            result = workspace.statement_execution.get_statement(result.statement_id)
            print("Waiting for query to complete...")
            state = result.status.state
            print(state)
            if state in [StatementState.SUCCEEDED, StatementState.FAILED, StatementState.CANCELED]:
                break
            time.sleep(1)

        # print(result.result)
        # print("-----")
        # print(result.result.data_array)

        # Process and display results
        if result.result and result.result.data_array:
            display_job_costs(result.result.data_array)
            display_summary(result.result.data_array)
        else:
            print(f"No job runs found for {target_date}")

    except Exception as e:
        print(f"Error querying system tables: {e}")
        fallback_analysis(workspace, target_date)


def get_warehouse_id(workspace: WorkspaceClient) -> str:
    """Get the first available SQL warehouse ID."""
    warehouses = list(workspace.warehouses.list())
    if not warehouses:
        raise ValueError("No SQL warehouse found. Please create one to query system tables.")
    return warehouses[0].id


def display_job_costs(data_array):
    print(f"{'Job ID':<15} {'Job Name':<40} {'SKU':<40} {'Runs':>8} {'DBUs':>10} {'Cost (USD)':>12}")
    print("-" * 110)

    for row in data_array:
        job_id = row[0] or "N/A"
        job_name = row[1] or "Unnamed"
        sku = row[2] or "Unnamed"
        total_dbu = float(row[4] or 0)
        num_runs = int(row[5] or 0)
        cost = float(row[6] or 0)

        print(f"{str(job_id):<15} {job_name:<40.40} {sku:<40.40} {num_runs:>8} {total_dbu:>10.2f} ${cost:>11.2f}")


def display_summary(data_array):
    print("\nSUMMARY")
    print("=" * 80)

    total_jobs = len(set([row[0] for row in data_array if row[0]]))
    total_dbu = sum([float(row[4] or 0) for row in data_array])
    # total_runs = sum([int(row[5] or 0) for row in data_array])
    total_cost = sum([float(row[6] or 0) for row in data_array])

    print(f"Total Jobs:      {total_jobs}")
    # print(f"Total Runs:      {total_runs}")
    print(f"Total DBUs:      {total_dbu:.2f}")
    print(f"Total Cost:      ${total_cost:.2f}")
    print("=" * 80 + "\n")


def fallback_analysis(workspace: WorkspaceClient, target_date: str):
    """
    Fallback method using Jobs API when system tables are unavailable.
    Note: This doesn't provide cost data, only run information.
    """
    print("\nFallback: Using Jobs API for run information (costs not available)\n")

    target_datetime = datetime.strptime(target_date, "%Y-%m-%d")
    start_time_ms = int(target_datetime.timestamp() * 1000)
    end_time_ms = int((target_datetime + timedelta(days=1)).timestamp() * 1000)

    print(f"{'Job ID':<15} {'Job Name':<40} {'Status':<15} {'Start Time':<20}")
    print("-" * 95)

    job_count = 0
    run_count = 0

    try:
        # List all jobs
        for job in workspace.jobs.list():
            # Get runs for this job
            runs = workspace.jobs.list_runs(job_id=job.job_id, start_time_from=start_time_ms, start_time_to=end_time_ms)

            for run in runs:
                if run.start_time and start_time_ms <= run.start_time < end_time_ms:
                    job_name = job.settings.name if job.settings and job.settings.name else "Unnamed"
                    if len(job_name) > 38:
                        job_name = job_name[:35] + "..."

                    start_time = datetime.fromtimestamp(run.start_time / 1000).strftime("%Y-%m-%d %H:%M:%S")
                    status = run.state.life_cycle_state if run.state else "UNKNOWN"

                    print(f"{job.job_id:<15} {job_name:<40} {status:<15} {start_time:<20}")
                    run_count += 1

            if runs:
                job_count += 1

        print(f"\nFound {run_count} runs across {job_count} jobs")
        print("Note: Cost information requires system.billing.usage table access\n")

    except Exception as e:
        print(f"Error accessing Jobs API: {e}\n")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Analyze Databricks job costs for today",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/analyze_job_costs.py
  python scripts/analyze_job_costs.py --profile dev
  python scripts/analyze_job_costs.py --date 2024-01-15
        """,
    )

    parser.add_argument("--profile", default="dev", help="Databricks CLI profile to use (default: dev)")

    parser.add_argument(
        "--date", default=get_todays_date(), help="Date to analyze in YYYY-MM-DD format (default: today)"
    )

    args = parser.parse_args()

    # Validate date format
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"Error: Invalid date format '{args.date}'. Use YYYY-MM-DD")
        return 1

    # Initialize Databricks workspace client
    try:
        workspace = WorkspaceClient(profile=args.profile)
        print(f"Connected to: {workspace.config.host}")
    except Exception as e:
        print(f"Error connecting to Databricks: {e}")
        print(f"Ensure profile '{args.profile}' exists in ~/.databrickscfg")
        return 1

    # Run cost analysis
    analyze_job_costs(workspace, args.date)

    return 0


if __name__ == "__main__":
    exit(main())
