"""
Show AWS and Databricks spend for the last 30 days, week by week.
Usage: uv run python scripts/project_costs.py [--days N] [--profile PROFILE] [--aws-profile PROFILE]

Also writes cost_report/YYYY-MM-DD.md (gitignored) holding the same tables in markdown, with an
empty Analysis section for the /project-costs skill to fill in. The script never writes prose —
it emits only numbers it pulled, so the report cannot drift from the data.
"""

import argparse
import json
import re
import subprocess
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

_POLL_TERMINAL = {StatementState.SUCCEEDED, StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED}

_REPORT_DIR = Path(__file__).resolve().parent.parent / "cost_report"

# Native quantities span DBU / DSU / GB and are not comparable with each other, so the Quantity
# column is never totalled. USD is the only figure that spans SKUs and clouds.
_UNIT_NOTE = "Quantity spans DBU/DSU/GB and is never totalled; only USD is comparable across SKUs and clouds."
_PRICE_NOTE = (
    "Databricks USD is list price (system.billing.list_prices, effective_list) — it excludes "
    "account discounts and commit contracts, so treat it as an upper bound."
)
_WEEK_NOTE = "Weeks start Monday; the first and last weeks in the window are usually partial."


def _money(v: float) -> str:
    return f"{v:.4f}"


def _week_of(dates: pd.Series) -> pd.Series:
    """Bucket dates into Monday-start weeks, labelled by the week's Monday."""
    return pd.to_datetime(dates).dt.to_period("W-SUN").dt.start_time.dt.date.astype(str)


def _short_service(name: str) -> str:
    """'Amazon Simple Storage Service' -> 'Simple Storage Service' — fit service names into columns."""
    return re.sub(r"^(Amazon|AWS)\s+", "", name)


def _short_sku(name: str) -> str:
    """'PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST_OHIO' -> 'JOBS_SERVERLESS_COMPUTE'.

    Strips the tier prefix and the trailing cloud region so SKUs fit as column headers.
    """
    return re.sub(r"_(US|EU|AP|CA|SA|AF|ME)_[A-Z0-9_]+$", "", re.sub(r"^PREMIUM_", "", name))


def _md_table(df: pd.DataFrame, index_label: str | None = None) -> str:
    """Render a DataFrame as a GitHub-flavoured markdown table.

    Hand-rolled rather than DataFrame.to_markdown() so the script keeps working without adding a
    `tabulate` dependency for one table format.
    """
    header = ([index_label] if index_label else []) + [str(c) for c in df.columns]
    rows = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    for idx, row in df.iterrows():
        cells = ([str(idx)] if index_label else []) + [_cell(v) for v in row]
        rows.append("| " + " | ".join(cells) + " |")
    return "\n".join(rows)


def _cell(v: object) -> str:
    """Format one table cell. NaN renders as an em dash, not the string 'nan'."""
    if isinstance(v, float):
        return "—" if pd.isna(v) else _money(v)
    return str(v)


# --------------------------------------------------------------------------------------------
# Fetch
# --------------------------------------------------------------------------------------------


def fetch_aws(days: int, profile: str | None = None) -> pd.DataFrame | None:
    """Pull per-day, per-service blended cost from Cost Explorer. Returns None on any failure."""
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
        return None

    if result.returncode != 0:
        # Don't echo raw stderr — AWS error messages can include caller ARNs with account IDs.
        print("AWS error: unable to retrieve cost data (check AWS credentials and ce:GetCostAndUsage permission).\n")
        return None

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        print("AWS error: unexpected response from AWS CLI (pager or credential helper output?).\n")
        return None

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
    if aws_df.empty:
        print("AWS: no cost data returned for this window.\n")
        return None

    aws_df["week"] = _week_of(aws_df["date"])
    return aws_df


def fetch_databricks(profile: str, days: int) -> pd.DataFrame | None:
    """Pull per-day, per-SKU usage and list-price cost. Returns None on any failure.

    Usage quantities are monetized by joining system.billing.list_prices. Two things about that
    join are load-bearing:

    * It must be scoped to the usage date. list_prices is a slowly-changing dimension — one SKU
      holds several rows with different validity windows — so joining on sku_name alone fans out
      and multiplies the usage (measured at 3.5x on JOBS_SERVERLESS_COMPUTE).
    * It must be a LEFT join. An inner join would make usage for an unpriced SKU vanish from the
      output entirely rather than show up with a blank cost.

    pricing.effective_list.default resolves list against any active promotional price, and is the
    field Databricks documents for costing. These are LIST prices: account-level discounts and
    commit contracts are not exposed here, so the USD figures are an upper bound.
    """
    try:
        w = WorkspaceClient(profile=profile)
    except Exception as e:
        # Avoid printing the exception directly — SDK exceptions can include the workspace URL.
        print(f"Could not connect to Databricks ({profile} profile): {type(e).__name__}\n")
        return None

    warehouses = list(w.warehouses.list())
    if not warehouses:
        print("No SQL warehouse available.\n")
        return None
    warehouse_id = warehouses[0].id

    # No ROUND() here: rounding per day and then summing 30 days destroys small SKUs — internet
    # egress bills ~$0.000009/day, which rounds to 0.0000 every single day and totals to nothing.
    # Keep full precision and round only at display time.
    sql = f"""
    SELECT
      u.usage_date,
      u.sku_name,
      SUM(u.usage_quantity) AS total_quantity,
      u.usage_unit,
      SUM(u.usage_quantity * p.pricing.effective_list.default) AS usd,
      SUM(CASE WHEN p.sku_name IS NULL THEN 1 ELSE 0 END) AS unpriced_records
    FROM system.billing.usage u
    LEFT JOIN system.billing.list_prices p
      ON u.sku_name = p.sku_name
     AND u.usage_end_time >= p.price_start_time
     AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
    WHERE u.usage_date >= CURRENT_DATE() - INTERVAL {days} DAYS
    GROUP BY u.usage_date, u.sku_name, u.usage_unit
    ORDER BY u.usage_date DESC, usd DESC
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
            return None

        # Numbers arrive as strings from the SQL result.
        usage_df = pd.DataFrame(
            rows,
            columns=["usage_date", "sku_name", "total_quantity", "usage_unit", "usd", "unpriced_records"],
        )
        usage_df["total_quantity"] = usage_df["total_quantity"].astype(float)
        usage_df["usd"] = usage_df["usd"].astype(float)
        usage_df["unpriced_records"] = usage_df["unpriced_records"].astype(int)
        usage_df["week"] = _week_of(usage_df["usage_date"])
        usage_df["sku_short"] = usage_df["sku_name"].map(_short_sku)
        # dropna=False downstream keeps a NULL-unit SKU visible; give it a printable label here.
        usage_df["usage_unit"] = usage_df["usage_unit"].fillna("(none)")

        # A SKU missing from list_prices would otherwise be silently costed at $0 — say so.
        unpriced = usage_df.loc[usage_df["unpriced_records"] > 0, "sku_name"].unique()
        if len(unpriced):
            print(f"Warning: no list price found for {', '.join(unpriced)} — USD understated.\n")

        return usage_df.drop(columns=["unpriced_records"])

    except Exception as e:
        print(f"system.billing.usage / list_prices not available: {e}")
        print("Free-tier workspaces don't expose system.billing (requires Premium).\n")
        return None


# --------------------------------------------------------------------------------------------
# Aggregate — builders shared by the stdout report and the markdown report
# --------------------------------------------------------------------------------------------


def aws_weekly(aws_df: pd.DataFrame) -> pd.DataFrame:
    """Weeks as rows, services as columns, plus a Total column (all values are USD)."""
    # Zero-cost services (free tier / idle) would be dead columns; the by-service rollup is the
    # place to confirm they really are $0.
    weekly = aws_df.pivot_table(
        index="week",
        columns=aws_df["service"].map(_short_service),
        values="blended_cost_usd",
        aggfunc="sum",
        fill_value=0.0,
    )
    weekly = weekly.loc[:, weekly.sum() > 0]
    weekly["Total"] = weekly.sum(axis=1)

    estimated_weeks = set(aws_df.loc[aws_df["estimated"], "week"])
    weekly.index = pd.Index([f"{w}*" if w in estimated_weeks else w for w in weekly.index], name="Week")
    weekly.columns.name = None
    return weekly


def dbx_weekly(usage_df: pd.DataFrame) -> pd.DataFrame:
    """Weeks as rows, SKUs as columns, in USD, plus a Total column.

    Monetizing is what makes a Total meaningful here: raw quantities span DBU/DSU/GB and bill at
    different rates, so they can only be totalled once converted to a common currency.
    """
    weekly = usage_df.pivot_table(
        index="week",
        columns="sku_short",
        values="usd",
        aggfunc="sum",
        fill_value=0.0,
    )
    weekly = weekly.loc[:, weekly.sum() > 0]
    weekly["Total"] = weekly.sum(axis=1)
    weekly.index.name = "Week"
    weekly.columns.name = None
    return weekly


def dbx_by_sku(usage_df: pd.DataFrame) -> pd.DataFrame:
    """Per-SKU rollup keeping native quantity + unit alongside the monetized cost."""
    return (
        usage_df.groupby(["sku_name", "usage_unit"], as_index=False, dropna=False)
        .agg(Quantity=("total_quantity", "sum"), USD=("usd", "sum"))
        .rename(columns={"sku_name": "SKU", "usage_unit": "Unit"})
        .sort_values("USD", ascending=False, ignore_index=True)[["SKU", "Quantity", "Unit", "USD"]]
    )


def combined_totals(aws_df: pd.DataFrame | None, usage_df: pd.DataFrame | None) -> pd.DataFrame | None:
    """One row per service/SKU across both clouds, monetized to USD so the two are comparable.

    Quantity/Unit describe native usage and are blank for AWS, where Cost Explorer reports spend
    directly rather than a usage quantity. USD is the only column that spans both clouds, so it is
    the only one totalled. Zero-usage services are dropped, matching the per-cloud rollups.
    """
    parts = []
    if aws_df is not None:
        aws_part = (
            aws_df.groupby("service", as_index=False)["blended_cost_usd"]
            .sum()
            .rename(columns={"service": "Service", "blended_cost_usd": "USD"})
        )
        aws_part.insert(0, "Cloud", "AWS")
        aws_part["Quantity"] = float("nan")
        aws_part["Unit"] = "—"
        parts.append(aws_part)

    if usage_df is not None:
        dbx_part = dbx_by_sku(usage_df).rename(columns={"SKU": "Service"})
        dbx_part.insert(0, "Cloud", "Databricks")
        parts.append(dbx_part)

    if not parts:
        return None

    combined = pd.concat(parts, ignore_index=True)[["Cloud", "Service", "Quantity", "Unit", "USD"]]
    combined = combined[combined["USD"] > 0]
    return combined.sort_values(["Cloud", "USD"], ascending=[True, False], ignore_index=True)


# --------------------------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------------------------


def print_aws(aws_df: pd.DataFrame, days: int) -> None:
    print(f"\nAWS Costs by Week × Service — last {days} days (USD)")
    print(aws_weekly(aws_df).to_string(float_format=_money))
    print("* = week contains estimated (not-yet-finalized) days")
    print(f"{_WEEK_NOTE}\n")


def print_databricks(usage_df: pd.DataFrame, days: int) -> None:
    print(f"\nDatabricks Costs by Week × SKU — last {days} days (USD, list price)")
    print(dbx_weekly(usage_df).to_string(float_format=_money))
    print(f"{_WEEK_NOTE}")
    print(f"{_PRICE_NOTE}\n")


def print_combined(combined: pd.DataFrame, days: int) -> None:
    print(f"Combined Totals by Service — last {days} days")
    print(f"{'Cloud':<12} {'Service':<48} {'Quantity':>12}  {'Unit':<5} {'USD':>10}")
    print("-" * 92)
    for r in combined.itertuples(index=False):
        qty = "—".rjust(12) if pd.isna(r.Quantity) else f"{r.Quantity:>12.4f}"
        print(f"{r.Cloud:<12} {r.Service:<48.48} {qty}  {r.Unit:<5} {r.USD:>10.4f}")
    print("-" * 92)
    print(f"{'Total':<12} {'':<48} {'':>12}  {'':<5} {combined['USD'].sum():>10.4f}")
    print(_UNIT_NOTE)
    print(_PRICE_NOTE)
    print()


def write_markdown(
    aws_df: pd.DataFrame | None,
    usage_df: pd.DataFrame | None,
    combined: pd.DataFrame | None,
    days: int,
) -> Path:
    """Write the data tables to cost_report/YYYY-MM-DD.md, leaving Analysis for the skill."""
    _REPORT_DIR.mkdir(exist_ok=True)
    path = _REPORT_DIR / f"{date.today()}.md"

    end = date.today()
    start = end - timedelta(days=days)
    out = [
        f"# Cost Report — {end}",
        "",
        f"Window: **{start} → {end}** ({days} days). Generated by `scripts/project_costs.py`.",
        "",
        f"> {_UNIT_NOTE}",
        f"> {_PRICE_NOTE}",
        f"> {_WEEK_NOTE}",
        "",
        "## Combined Totals by Service",
        "",
    ]
    if combined is not None:
        headline = combined.groupby("Cloud")["USD"].sum()
        out += [
            "Total: **$"
            + f"{combined['USD'].sum():.2f}**  ("
            + ", ".join(f"{cloud} ${amt:.2f}" for cloud, amt in headline.items())
            + ")",
            "",
            _md_table(combined),
        ]
    else:
        out.append("_No data available._")
    out.append("")

    if aws_df is not None:
        estimated = (
            " Weeks marked `*` contain estimated (not-yet-finalized) days." if bool(aws_df["estimated"].any()) else ""
        )
        out += [
            "## AWS — by Week × Service (USD)",
            "",
            f"Total: **${aws_df['blended_cost_usd'].sum():.4f}** over {days} days.{estimated}",
            "",
            _md_table(aws_weekly(aws_df), index_label="Week"),
            "",
            # Collapsed, so it costs nothing visually — but the weekly pivot averages single-day
            # spikes away, and this is the only place they can be attributed to a date.
            "<details><summary>Daily detail (non-zero rows)</summary>",
            "",
            _md_table(
                aws_df[aws_df["blended_cost_usd"] > 0]
                .drop(columns=["week"])
                .reset_index(drop=True)
                .rename(
                    columns={
                        "date": "Date",
                        "service": "Service",
                        "blended_cost_usd": "USD",
                        "estimated": "Estimated",
                    }
                )
            ),
            "",
            "</details>",
            "",
        ]

    if usage_df is not None:
        out += [
            "## Databricks — by Week × SKU (USD)",
            "",
            f"Total: **${usage_df['usd'].sum():.2f}** over {days} days at list price. Native DBU/DSU"
            " quantities are in the Combined Totals table above.",
            "",
            _md_table(dbx_weekly(usage_df), index_label="Week"),
            "",
            "<details><summary>Daily detail</summary>",
            "",
            _md_table(
                usage_df.drop(columns=["week", "sku_short"])
                .reset_index(drop=True)
                .rename(
                    columns={
                        "usage_date": "Date",
                        "sku_name": "SKU",
                        "total_quantity": "Quantity",
                        "usage_unit": "Unit",
                        "usd": "USD",
                    }
                )
            ),
            "",
            "</details>",
            "",
        ]

    out += ["## Analysis", "", "_Not yet written — run the `/project-costs` skill to fill this in._", ""]

    path.write_text("\n".join(out))
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Show AWS and Databricks spend week by week")
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

    aws_df = fetch_aws(args.days, args.aws_profile)
    if aws_df is not None:
        print_aws(aws_df, args.days)

    print(f"Databricks Costs — last {args.days} days")
    usage_df = fetch_databricks(args.profile, args.days)
    if usage_df is not None:
        print_databricks(usage_df, args.days)

    combined = combined_totals(aws_df, usage_df)
    if combined is not None:
        print_combined(combined, args.days)

    path = write_markdown(aws_df, usage_df, combined, args.days)
    print(f"Report written to {path.relative_to(Path.cwd()) if path.is_relative_to(Path.cwd()) else path}\n")


if __name__ == "__main__":
    main()
