"""
Pure transformation functions for the job1_sdp Spark Declarative Pipeline.

These functions are SDP-runtime-agnostic: they take DataFrames as arguments and
return DataFrames, with no references to `spark` globals or `@dp.*` decorators.
This makes them directly testable with in-memory DataFrames.

The SDP wiring (pipeline.py) imports these functions and calls them from inside
@dp.table / @dp.materialized_view decorated functions.
"""

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import Criticality, DQDatasetRule, DQForEachColRule, DQRowRule
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_dq_checks() -> list:
    """
    Return the DQX rule list for external_source.order validation.

    Rules mirror ExtractSource2.validate_order exactly so both the batch job
    and the SDP pipeline enforce the same data-quality contract.

    - WARN  : total > 150 (kept as valid, flagged in _warnings)
    - ERROR : id or id_customer is null / empty (quarantined)
    - ERROR : id is not unique across the dataset (quarantined)
    """
    return [
        # Warning if total > 150
        DQRowRule(
            column="total",
            check_func=check_funcs.is_not_greater_than,
            check_func_kwargs={"limit": 150},
            criticality=Criticality.WARN.value,
        ),
        # Error if id or id_customer are null or empty
        *DQForEachColRule(
            columns=["id", "id_customer"],
            check_func=check_funcs.is_not_null_and_not_empty,
            criticality=Criticality.ERROR.value,
            user_metadata={
                "check_type": "completeness",
                "responsible_data_steward": "someone@email.com",
            },
        ).get_rules(),
        # Error if id is not unique
        DQDatasetRule(
            columns=["id"],
            check_func=check_funcs.is_unique,
            criticality=Criticality.ERROR.value,
        ),
    ]


def validate_order(dq_engine, df: DataFrame) -> DataFrame:
    """
    Apply DQX checks to the orders DataFrame.

    Returns the input DataFrame annotated with ``_errors`` and ``_warnings``
    array columns.  Callers can then split via ``dq_engine.get_valid()`` /
    ``dq_engine.get_invalid()``.

    Args:
        dq_engine: Instantiated ``DQEngine`` (can be mocked in unit tests).
        df:        Source orders DataFrame.

    Returns:
        DataFrame with ``_errors`` and ``_warnings`` columns added.
    """
    return dq_engine.apply_checks(df, build_dq_checks())


def enrich_order(df_customer: DataFrame, df_order: DataFrame, df_order_item: DataFrame) -> DataFrame:
    """
    Three-way join: order_item ⨝ order ⨝ customer.

    Mirrors GenerateOrders.enrich_order exactly.

    Args:
        df_customer:   raw.customer
        df_order:      raw.order  (valid rows only, DQX-filtered)
        df_order_item: raw.order_item

    Returns:
        Enriched DataFrame with columns:
        name, id_customer, id_order, total, seq, desc_item, qty, total_item
    """
    return (
        df_order_item.join(df_order, df_order_item["id_order"] == df_order["id"])
        .join(df_customer, df_order["id_customer"] == df_customer["id"])
        .select("name", "id_customer", "id_order", "total", "seq", "desc_item", "qty", "total_item")
    )


def aggregate_orders(df_order_enriched: DataFrame) -> DataFrame:
    """
    Aggregate enriched orders by customer name.

    Mirrors GenerateOrdersAgg.aggregate_orders exactly.

    Args:
        df_order_enriched: curated.order_enriched

    Returns:
        DataFrame with columns: name, total_qty (LongType), total_value (DoubleType)
    """
    return df_order_enriched.groupBy("name").agg(
        F.sum("qty").alias("total_qty"),
        F.sum("total_item").alias("total_value"),
    )
