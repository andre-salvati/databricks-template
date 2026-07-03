"""
Unit tests for job1_sdp transformation functions.

These tests import only from template.job1_sdp.transforms — the pure,
SDP-runtime-agnostic functions.  pipeline.py is intentionally not imported
here: it references the `spark` global injected by the SDP runtime and would
raise NameError in a plain pytest session.

The test structure and fixtures mirror tests/job1/unit_test.py so the two
pipelines are held to the same data contract.
"""

from datetime import date

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.testing import assertDataFrameEqual

from template.commonSchemas import (
    customer_schema,
    order_agg_schema,
    order_enriched_schema,
    order_item_schema,
    order_schema,
    product_schema,
)
from template.job1_sdp.transforms import aggregate_orders, enrich_order


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return SparkSession.builder.appName("job1-sdp-unit-tests").getOrCreate()


@pytest.fixture
def df_orders_enriched(spark) -> DataFrame:
    """Pre-joined enriched orders matching the output of enrich_order()."""
    data = [
        (
            "John Doe",
            "USA",
            10,
            1,
            100.0,
            date(2023, 1, 1),
            1,
            "Product 1",
            1,
            "Category 1",
            1,
            "Item A",
            2,
            50.0,
        ),
        (
            "John Doe",
            "USA",
            10,
            1,
            100.0,
            date(2023, 1, 1),
            1,
            "Product 1",
            1,
            "Category 1",
            2,
            "Item B",
            1,
            50.0,
        ),
        (
            "Jane Smith",
            "UK",
            20,
            2,
            150.0,
            date(2023, 1, 2),
            2,
            "Product 2",
            2,
            "Category 2",
            1,
            "Item C",
            3,
            150.0,
        ),
    ]
    return spark.createDataFrame(data, schema=order_enriched_schema)


# ── enrich_order ──────────────────────────────────────────────────────────────


def test_enrich_order_row_count(spark, df_orders_enriched):
    """One enriched row per order-item line."""
    df_customer = spark.createDataFrame(
        [(10, "John Doe", "USA"), (20, "Jane Smith", "UK")],
        schema=customer_schema,
    )
    df_order = spark.createDataFrame(
        [(1, 10, 100.0, "2023-01-01", 1), (2, 20, 150.0, "2023-01-02", 2)],
        schema=order_schema,
    )
    df_order_item = spark.createDataFrame(
        [(1, 1, "Item A", 2, 50.0), (1, 2, "Item B", 1, 50.0), (2, 1, "Item C", 3, 150.0)],
        schema=order_item_schema,
    )
    df_product = spark.createDataFrame(
        [(1, "Product 1", 10.0, 1, "Category 1"), (2, "Product 2", 25.0, 2, "Category 2")],
        schema=product_schema,
    )

    df_out = enrich_order(df_customer, df_order, df_order_item, df_product)

    assert df_out.count() == 3
    assertDataFrameEqual(df_out, df_orders_enriched)


def test_enrich_order_columns(spark):
    """Output schema must contain exactly the declared columns."""
    expected_cols = {
        "customer_name",
        "country",
        "customer_id",
        "order_id",
        "order_total",
        "order_date",
        "product_id",
        "product_name",
        "product_category_id",
        "category_name",
        "item_seq",
        "item_description",
        "item_quantity",
        "item_total",
    }

    df_customer = spark.createDataFrame([(10, "Alice", "US")], schema=customer_schema)
    df_order = spark.createDataFrame([(1, 10, 50.0, "2024-01-01", 1)], schema=order_schema)
    df_order_item = spark.createDataFrame([(1, 1, "Widget", 1, 50.0)], schema=order_item_schema)
    df_product = spark.createDataFrame([(1, "Product 1", 9.99, 1, "Category 1")], schema=product_schema)

    df_out = enrich_order(df_customer, df_order, df_order_item, df_product)
    assert set(df_out.columns) == expected_cols


# ── aggregate_orders ──────────────────────────────────────────────────────────


def test_aggregate_orders_row_count(spark, df_orders_enriched):
    """One aggregated row per distinct customer name."""
    df_out = aggregate_orders(df_orders_enriched)
    assert df_out.count() == 2


def test_aggregate_orders_values(spark, df_orders_enriched):
    """Aggregated qty and value must match hand-calculated totals."""
    df_out = aggregate_orders(df_orders_enriched)

    expected_data = [
        ("John Doe", "USA", date(2023, 1, 1), 1, "Product 1", 1, "Category 1", 3, 100.0, 1),  # item_total 50+50=100
        ("Jane Smith", "UK", date(2023, 1, 2), 2, "Product 2", 2, "Category 2", 3, 150.0, 1),  # item_total 150
    ]
    df_expected = spark.createDataFrame(expected_data, schema=order_agg_schema)

    assertDataFrameEqual(df_out, df_expected)
