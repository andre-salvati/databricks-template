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
        customer_name, country, customer_id, order_id, order_total, order_date, product_id,
        product_category_id, item_seq, item_description, item_quantity, item_total
    """
    return (
        df_order_item.join(df_order, df_order_item["id_order"] == df_order["id"])
        .join(df_customer, df_order["id_customer"] == df_customer["id"])
        .select(
            df_customer["name"].alias("customer_name"),
            "country",
            df_order["id_customer"].alias("customer_id"),
            df_order_item["id_order"].alias("order_id"),
            df_order["total"].alias("order_total"),
            df_order["date"].cast("date").alias("order_date"),
            "product_id",
            df_order["prod_category_id"].alias("product_category_id"),
            df_order_item["seq"].alias("item_seq"),
            df_order_item["desc_item"].alias("item_description"),
            df_order_item["qty"].alias("item_quantity"),
            df_order_item["total_item"].alias("item_total"),
        )
    )


def aggregate_orders(df_order_enriched: DataFrame) -> DataFrame:
    """
    Aggregate enriched orders by customer name.

    Mirrors GenerateOrdersAgg.aggregate_orders exactly.

    Args:
        df_order_enriched: curated.order_enriched

    Returns:
        DataFrame with columns: customer_name, country, order_date, product_id,
        product_category_id, total_quantity (LongType), total_value (DoubleType), total_orders (LongType)
    """
    return df_order_enriched.groupBy("customer_name", "country", "order_date", "product_id", "product_category_id").agg(
        F.sum("item_quantity").alias("total_quantity"),
        F.sum("item_total").alias("total_value"),
        F.countDistinct("order_id").alias("total_orders"),
    )
