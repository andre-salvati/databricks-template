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


def enrich_order(
    df_customer: DataFrame, df_order: DataFrame, df_order_item: DataFrame, df_product: DataFrame
) -> DataFrame:
    """
    Four-way join: order_item ⨝ order ⨝ customer ⨝ product.

    Mirrors GenerateOrders.enrich_order exactly. line_revenue = item_quantity × unit_price
    is computed from the product dimension's current price; the streaming materialization
    in pipeline.py then freezes it (each order_item row is appended once, never
    reprocessed) — matching the INSERT-only MERGE freeze on the batch path.

    Args:
        df_customer:   raw.customer_sdp
        df_order:      raw.order_sdp
        df_order_item: raw.order_item_sdp  (the streaming fact)
        df_product:    raw.product_sdp     (static dimension — current price)

    Returns:
        Enriched DataFrame with columns:
        customer_name, country, customer_id, order_id, order_total, order_date, product_id,
        product_name, product_category_id, category_name, item_seq, item_description,
        item_quantity, item_total, line_revenue, unit_price_at_sale
    """
    return (
        df_order_item.join(df_order, df_order_item["id_order"] == df_order["id"])
        .join(df_customer, df_order["id_customer"] == df_customer["id"])
        .join(df_product, df_order["product_id"] == df_product["product_id"])
        .select(
            df_customer["name"].alias("customer_name"),
            "country",
            df_order["id_customer"].alias("customer_id"),
            df_order_item["id_order"].alias("order_id"),
            df_order["total"].alias("order_total"),
            df_order["date"].cast("date").alias("order_date"),
            df_order["product_id"],
            df_product["name"].alias("product_name"),
            df_product["category_id"].alias("product_category_id"),
            df_product["category_name"],
            df_order_item["seq"].alias("item_seq"),
            df_order_item["desc_item"].alias("item_description"),
            df_order_item["qty"].alias("item_quantity"),
            df_order_item["total_item"].alias("item_total"),
            (df_order_item["qty"] * df_product["unit_price"]).cast("double").alias("line_revenue"),
            df_product["unit_price"].alias("unit_price_at_sale"),
        )
    )


def aggregate_orders(df_order_enriched: DataFrame) -> DataFrame:
    """
    Aggregate enriched orders by the report dimensions.

    Mirrors GenerateOrdersAgg.aggregate_orders exactly. total_value sums the frozen
    line_revenue, not the raw item_total.

    Args:
        df_order_enriched: curated.order_enriched_sdp

    Returns:
        DataFrame with columns: customer_name, country, order_date, product_id, product_name,
        product_category_id, category_name, total_quantity (LongType), total_value (DoubleType),
        total_orders (LongType)
    """
    return df_order_enriched.groupBy(
        "customer_name",
        "country",
        "order_date",
        "product_id",
        "product_name",
        "product_category_id",
        "category_name",
    ).agg(
        F.sum("item_quantity").alias("total_quantity"),
        F.sum("line_revenue").alias("total_value"),
        F.countDistinct("order_id").alias("total_orders"),
    )
