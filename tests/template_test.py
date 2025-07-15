from argparse import Namespace

import pytest
from pyspark.sql import *
from pyspark.sql.types import *

from template.main import *
from template.config import Config as TaskConfig
from template.generate_orders import GenerateOrders
from template.generate_orders_agg import GenerateOrdersAgg
from template.commonSchemas import customer_schema, order_schema, order_item_schema

from pyspark.testing import assertDataFrameEqual

# from databricks.connect import DatabricksSession
# from databricks.sdk.core import Config


@pytest.fixture
def spark() -> DataFrame:
    # config = Config(profile = "DEV")
    # return DatabricksSession.builder.sdkConfig(config).getOrCreate()
    return SparkSession.builder.appName("unit-tests").getOrCreate()


@pytest.fixture
def config() -> TaskConfig:
    return TaskConfig(
        Namespace(
            task="extract_source1",
            env="local",
            default_catalog="dev",
            schema="raw",
            skip=False,
            debug=True,
        )
    )


@pytest.fixture
def spark(config) -> TaskConfig:
    return config.get_spark()


@pytest.fixture
def df_orders_from_source(spark) -> DataFrame:
    order_data = [
        (1, 10, 100.0, "2023-01-01"),
        (2, 20, 151.0, "2023-01-02"),
        (None, 10, 100.0, "2023-01-01"),  # id is null
        (3, 20, 150.0, "2023-01-02"),  # id is duplicated
        (3, 20, 150.0, "2023-01-02"),  # id is duplicated
    ]
    return spark.createDataFrame(order_data, schema=order_schema)


@pytest.fixture
def df_orders(spark) -> DataFrame:
    orders_data = [
        ("John Doe", 10, 1, 100.0, 1, "Item A", 2, 50.0),
        ("John Doe", 10, 1, 100.0, 2, "Item B", 1, 50.0),
        ("Jane Smith", 20, 2, 150.0, 1, "Item C", 3, 150.0),
    ]
    orders_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("id_customer", IntegerType(), True),
            StructField("id_order", IntegerType(), True),
            StructField("total", FloatType(), True),
            StructField("seq", IntegerType(), True),
            StructField("desc_item", StringType(), True),
            StructField("qty", IntegerType(), True),
            StructField("total_item", FloatType(), True),
        ]
    )
    return spark.createDataFrame(orders_data, schema=orders_schema)


def test_arg_parser():
    parser = arg_parser()

    args = parser.parse_args(
        ["--task=extract_source1", "--user=andre_f_salvati", "--schema=raw", "--env=dev", "--skip", "--debug"]
    )

    assert args == Namespace(
        task="extract_source1", user="andre_f_salvati", schema="raw", env="dev", skip=True, debug=True
    )


@pytest.mark.parametrize(
    "args, expected_output",
    [
        (
            Namespace(
                task="extract_source1",
                env="local",
                skip=False,
                debug=True,
                schema="raw",
                user="andre_f_salvati",
            ),
            {
                "task": "extract_source1",
                "env": "local",
                "schema": "raw",
                "skip": False,
                "debug": True,
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
    df_out.show()
    df_out_invalid.show()

    assert df_out_invalid.count() == 4
    assert df_out.count() == 2


def test_enrich_orders(spark, config, df_orders):
    df_expected = df_orders

    task = GenerateOrders(config)

    customer_data = [(10, "John Doe", "USA"), (20, "Jane Smith", "UK")]
    df_customer = spark.createDataFrame(customer_data, schema=customer_schema)

    order_data = [(1, 10, 100.0, "2023-01-01"), (2, 20, 150.0, "2023-01-02")]
    df_order = spark.createDataFrame(order_data, schema=order_schema)

    order_item_data = [(1, 1, "Item A", 2, 50.0), (1, 2, "Item B", 1, 50.0), (2, 1, "Item C", 3, 150.0)]
    df_order_item = spark.createDataFrame(order_item_data, schema=order_item_schema)

    df_out = task.enrich_order(df_customer, df_order, df_order_item)

    assert df_out.count() == 3

    assertDataFrameEqual(df_out, df_expected)


def test_aggregate_orders(spark, config, df_orders):
    task = GenerateOrdersAgg(config)

    df_out = task.aggregate_orders(df_orders)

    assert df_out.count() == 2

    expected_data = [
        ("John Doe", 3, 100.0),
        ("Jane Smith", 3, 150.0),
    ]
    expected_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("total_qty", LongType(), True),
            StructField("total_value", DoubleType(), True),
        ]
    )
    df_expected = spark.createDataFrame(expected_data, schema=expected_schema)

    assertDataFrameEqual(df_out, df_expected)
