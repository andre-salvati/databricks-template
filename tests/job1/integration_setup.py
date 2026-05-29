from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType

from template.baseTask import BaseTask
from template.commonSchemas import customer_schema, order_item_schema, order_schema
from template.config import MEDALLION_SCHEMAS

SCHEMA = "external_source"


class Setup(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def _reset_schemas(self, catalog):
        # Wipe all medallion schemas (including ops) so the integration test starts
        # from a clean slate. Re-create them immediately: on staging/prod, Config no
        # longer creates schemas at runtime, so Setup is responsible for restoring
        # the full schema layout after the wipe.
        for s in MEDALLION_SCHEMAS:
            self.spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{s} CASCADE")
        for s in MEDALLION_SCHEMAS:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{s}")

    def _seed_standard(self, catalog):
        customer_data = [(10, "John Doe", "USA"), (20, "Jane Smith", "UK")]
        self.spark.createDataFrame(customer_data, schema=customer_schema).write.saveAsTable(
            f"{catalog}.{SCHEMA}.customer"
        )

        order_data = [
            (1, 10, 100.0, "2023-01-01"),
            (2, 20, 151.0, "2023-01-02"),
            (None, 10, 100.0, "2023-01-01"),  # id is null
            (3, 20, 150.0, "2023-01-02"),  # id is duplicated
            (3, 20, 150.0, "2023-01-02"),
        ]  # id is duplicated
        self.spark.createDataFrame(order_data, schema=order_schema).write.saveAsTable(f"{catalog}.{SCHEMA}.order")

        order_item_data = [(1, 1, "Item A", 2, 50.0), (1, 2, "Item B", 1, 50.0), (2, 1, "Item C", 3, 151.0)]
        self.spark.createDataFrame(order_item_data, schema=order_item_schema).write.saveAsTable(
            f"{catalog}.{SCHEMA}.order_item"
        )

    def _seed_load_test(self, catalog):
        # 200 customers (ids 1–200)
        self.spark.range(1, 201).select(
            F.col("id").cast(IntegerType()),
            F.concat(F.lit("Customer_"), F.col("id")).alias("name"),
            F.lit("USA").alias("country"),
        ).write.saveAsTable(f"{catalog}.{SCHEMA}.customer")

        # 500k orders — customer_id round-robins through 1–200
        self.spark.range(1, 500_001).select(
            F.col("id").cast(IntegerType()),
            ((F.col("id") - 1) % 200 + 1).cast(IntegerType()).alias("id_customer"),
            F.lit(100.0).cast(FloatType()).alias("total"),
            F.lit("2024-01-01").alias("date"),
        ).write.saveAsTable(f"{catalog}.{SCHEMA}.order")

        # 200 order_items — one item per order for orders 1–200
        self.spark.range(1, 201).select(
            F.col("id").cast(IntegerType()).alias("id_order"),
            F.lit(1).cast(IntegerType()).alias("seq"),
            F.concat(F.lit("Item_"), F.col("id")).alias("desc_item"),
            F.lit(2).cast(IntegerType()).alias("qty"),
            F.lit(50.0).cast(FloatType()).alias("total_item"),
        ).write.saveAsTable(f"{catalog}.{SCHEMA}.order_item")

    def run(self):
        load_test = self.config.get_value("load_test") == "true"
        self.logger.info("setup for integration tests (load_test=%s)", load_test)

        catalog = self.config.get_value("catalog")
        self._reset_schemas(catalog)

        if load_test:
            self._seed_load_test(catalog)
        else:
            self._seed_standard(catalog)
