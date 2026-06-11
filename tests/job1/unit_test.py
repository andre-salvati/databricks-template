from argparse import Namespace
from datetime import date

import pytest
from pyspark.sql import *
from pyspark.sql.types import *

from template.main import *
from template.config import Config as TaskConfig
from template.job1.generate_orders import GenerateOrders
from template.job1.generate_orders_agg import GenerateOrdersAgg
from template.job1.seed_sources import (
    SeedSources,
    _INCREMENTAL_ORDERS,
    _INCREMENTAL_CUSTOMER_UPDATES,
    _INCREMENTAL_PRICE_UPDATES,
)
from template.commonSchemas import (
    customer_schema,
    order_schema,
    order_item_schema,
    product_schema,
    order_enriched_schema,
    order_agg_schema,
)

from pyspark.testing import assertDataFrameEqual
from pyspark.sql.functions import explode

# from databricks.connect import DatabricksSession
# from databricks.sdk.core import Config


@pytest.fixture
def config() -> TaskConfig:
    return TaskConfig(
        Namespace(
            task="extract_source1",
            env="local",
            log_level="INFO",
            quarantine_fail_ratio=1.0,
            seed_date="2024-01-01",  # fixed date so tests are deterministic
        )
    )


@pytest.fixture
def spark(config) -> TaskConfig:
    return config.get_spark()


@pytest.fixture
def df_orders_from_source(spark) -> DataFrame:
    order_data = [
        (1, 10, 100.0, "2023-01-01", 1),
        (2, 20, 1001.0, "2023-01-02", 2),  # total > 1000 → WARN
        (None, 10, 100.0, "2023-01-01", 1),  # id is null
        (3, 20, 100.0, "2023-01-02", 3),  # id is duplicated
        (3, 20, 100.0, "2023-01-02", 3),  # id is duplicated
    ]
    return spark.createDataFrame(order_data, schema=order_schema)


@pytest.fixture
def df_orders(spark) -> DataFrame:
    # Enriched output of enrich_order: line_revenue = item_quantity × unit_price-at-sale.
    # product 1 = "Product 1" priced 10.0, product 2 = "Product 2" priced 25.0.
    orders_data = [
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
            20.0,
            10.0,
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
            10.0,
            10.0,
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
            75.0,
            25.0,
        ),
    ]
    return spark.createDataFrame(orders_data, schema=order_enriched_schema)


def test_arg_parser():
    parser = arg_parser()

    args = parser.parse_args(["--task=extract_source1", "--env=dev"])

    assert args == Namespace(
        task="extract_source1",
        env="dev",
        run_id=None,
        log_level="INFO",
        quarantine_fail_ratio=1.0,
        seed_date=None,
        load_test="false",
    )


@pytest.mark.parametrize(
    "args, expected_output",
    [
        (
            Namespace(
                task="extract_source1",
                env="local",
                log_level="INFO",
                quarantine_fail_ratio=1.0,
                seed_date="2024-01-01",
                load_test="false",
            ),
            {
                "task": "extract_source1",
                "env": "local",
                "log_level": "INFO",
                "quarantine_fail_ratio": 1.0,
                "seed_date": "2024-01-01",
                "load_test": "false",
            },
        ),
    ],
)
def test_config(args, expected_output):
    config = TaskConfig(args)

    assert config.get_test_output() == expected_output


def test_validate_orders_from_source(spark, config, df_orders_from_source):
    task = ExtractSource2(config)

    df_out, df_out_invalid = task.validate_order(df_orders_from_source)

    df_invalid_out = df_out_invalid.select("id", explode("_errors.name").alias("name")).union(
        df_out_invalid.select("id", explode("_warnings.name").alias("name"))
    )

    assert df_out.count() == 2
    assert df_invalid_out.count() == 4

    expected_data = [
        (None, "id_is_null_or_empty"),
        (2, "total_greater_than_limit"),
        (3, "id_is_not_unique"),
        (3, "id_is_not_unique"),
    ]
    expected_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ]
    )

    df_expected = spark.createDataFrame(expected_data, schema=expected_schema)

    df_invalid_out.show()
    df_expected.show()

    assertDataFrameEqual(df_invalid_out, df_expected)


def test_enrich_orders(spark, config, df_orders):
    df_expected = df_orders

    task = GenerateOrders(config)

    customer_data = [(10, "John Doe", "USA"), (20, "Jane Smith", "UK")]
    df_customer = spark.createDataFrame(customer_data, schema=customer_schema)

    order_data = [(1, 10, 100.0, "2023-01-01", 1), (2, 20, 150.0, "2023-01-02", 2)]
    df_order = spark.createDataFrame(order_data, schema=order_schema)

    order_item_data = [(1, 1, "Item A", 2, 50.0), (1, 2, "Item B", 1, 50.0), (2, 1, "Item C", 3, 150.0)]
    df_order_item = spark.createDataFrame(order_item_data, schema=order_item_schema)

    product_data = [(1, "Product 1", 10.0, 1, "Category 1"), (2, "Product 2", 25.0, 2, "Category 2")]
    df_product = spark.createDataFrame(product_data, schema=product_schema)

    df_out = task.enrich_order(df_customer, df_order, df_order_item, df_product)

    assert df_out.count() == 3

    assertDataFrameEqual(df_out, df_expected)


def test_aggregate_orders(spark, config, df_orders):
    task = GenerateOrdersAgg(config)

    df_out = task.aggregate_orders(df_orders)

    assert df_out.count() == 2

    # total_value = sum(line_revenue): John 20+10=30, Jane 75
    expected_data = [
        ("John Doe", "USA", date(2023, 1, 1), 1, "Product 1", 1, "Category 1", 3, 30.0, 1),
        ("Jane Smith", "UK", date(2023, 1, 2), 2, "Product 2", 2, "Category 2", 3, 75.0, 1),
    ]
    df_expected = spark.createDataFrame(expected_data, schema=order_agg_schema)

    assertDataFrameEqual(df_out, df_expected)


# ---------------------------------------------------------------------------
# SeedSources — builder methods only (no Spark tables, runs in local mode)
# ---------------------------------------------------------------------------


def test_seed_sources_incremental_orders_schema(spark, config):
    task = SeedSources(config)
    df = task._build_incremental_orders("2024-01-01")
    assert df.count() == _INCREMENTAL_ORDERS
    assert df.columns == [f.name for f in order_schema]
    dates = {row.date for row in df.collect()}
    assert dates == {"2024-01-01"}


def test_seed_sources_incremental_items_schema(spark, config):
    task = SeedSources(config)
    df = task._build_incremental_items("2024-01-01")
    assert df.count() == _INCREMENTAL_ORDERS
    assert df.columns == [f.name for f in order_item_schema]


def test_seed_sources_customer_updates_schema(spark, config):
    task = SeedSources(config)
    df = task._build_customer_updates("2024-01-01")
    assert df.count() == _INCREMENTAL_CUSTOMER_UPDATES
    assert df.columns == [f.name for f in customer_schema]


def test_seed_sources_price_updates_schema(spark, config):
    task = SeedSources(config)
    df = task._build_price_updates("2024-01-01")
    assert df.count() == _INCREMENTAL_PRICE_UPDATES
    assert df.columns == [f.name for f in product_schema]


def test_seed_sources_price_updates_deterministic_per_date(spark, config):
    """Same seed_date must produce identical price updates (idempotent reruns)."""
    task = SeedSources(config)
    a = {(r.product_id, r.unit_price) for r in task._build_price_updates("2024-01-05").collect()}
    b = {(r.product_id, r.unit_price) for r in task._build_price_updates("2024-01-05").collect()}
    assert a == b


def test_seed_sources_incremental_ids_unique_across_days(spark, config):
    """Order IDs generated for consecutive days must not collide."""
    task = SeedSources(config)
    ids_day0 = {row.id for row in task._build_incremental_orders("2024-01-01").collect()}
    ids_day1 = {row.id for row in task._build_incremental_orders("2024-01-02").collect()}
    assert ids_day0.isdisjoint(ids_day1)
