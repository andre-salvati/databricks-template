"""
Pure transformation functions for the job1_sdp Spark Declarative Pipeline.

These functions are SDP-runtime-agnostic: they take DataFrames as arguments and
return DataFrames, with no references to `spark` globals or `@dp.*` decorators.
This makes them directly testable with in-memory DataFrames.

The SDP wiring (pipeline.py) imports these functions and calls them from inside
@dp.materialized_view decorated functions.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def enrich_order(df_customer: DataFrame, df_order: DataFrame, df_order_item: DataFrame) -> DataFrame:
    """
    Three-way join: order_item ⨝ order ⨝ customer.

    Mirrors GenerateOrders.enrich_order exactly.

    Args:
        df_customer:   raw.customer
        df_order:      raw.order
        df_order_item: raw.order_item

    Returns:
        Enriched DataFrame with columns:
        name, id_customer, id_order, total, date, product_id, prod_category_id, seq, desc_item, qty, total_item
    """
    return (
        df_order_item.join(df_order, df_order_item["id_order"] == df_order["id"])
        .join(df_customer, df_order["id_customer"] == df_customer["id"])
        .select(
            "name",
            "id_customer",
            "id_order",
            "total",
            "date",
            "product_id",
            "prod_category_id",
            "seq",
            "desc_item",
            "qty",
            "total_item",
        )
    )


def aggregate_orders(df_order_enriched: DataFrame) -> DataFrame:
    """
    Aggregate enriched orders by customer name.

    Mirrors GenerateOrdersAgg.aggregate_orders exactly.

    Args:
        df_order_enriched: curated.order_enriched

    Returns:
        DataFrame with columns: name, date, product_id, prod_category_id, total_qty (LongType), total_value (DoubleType)
    """
    return df_order_enriched.groupBy("name", "date", "product_id", "prod_category_id").agg(
        F.sum("qty").alias("total_qty"),
        F.sum("total_item").alias("total_value"),
    )
