from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType

from template.baseTask import BaseTask
from template.commonSchemas import customer_schema, order_item_schema, order_schema, product_schema
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

        # Products referenced by the orders below (product_id 1, 2, 3).
        product_data = [
            (1, "Product 1", 10.0, 1, "Category 1"),
            (2, "Product 2", 20.0, 2, "Category 2"),
            (3, "Product 3", 30.0, 3, "Category 3"),
        ]
        self.spark.createDataFrame(product_data, schema=product_schema).write.saveAsTable(f"{catalog}.{SCHEMA}.product")

        order_data = [
            (1, 10, 100.0, "2023-01-01", 1),
            (2, 20, 1001.0, "2023-01-02", 2),  # total > 1000 → WARN
            (None, 10, 100.0, "2023-01-01", 1),  # id is null
            (3, 20, 150.0, "2023-01-02", 3),  # id is duplicated
            (3, 20, 150.0, "2023-01-02", 3),  # id is duplicated
        ]
        self.spark.createDataFrame(order_data, schema=order_schema).write.saveAsTable(f"{catalog}.{SCHEMA}.order")

        order_item_data = [(1, 1, "Item A", 2, 50.0), (1, 2, "Item B", 1, 50.0), (2, 1, "Item C", 3, 151.0)]
        self.spark.createDataFrame(order_item_data, schema=order_item_schema).write.saveAsTable(
            f"{catalog}.{SCHEMA}.order_item"
        )

    def _seed_load_test(self, catalog):
        # 500 customers (ids 1–500)
        self.spark.range(1, 501).select(
            F.col("id").cast(IntegerType()),
            F.concat(F.lit("Customer_"), F.col("id")).alias("name"),
            F.lit("USA").alias("country"),
        ).write.saveAsTable(f"{catalog}.{SCHEMA}.customer")

        # 100 products (ids 1–100). unit_price=25.0 so line_revenue = qty(2) × 25 = 50,
        # keeping the per-group total_value (120 items × 50 = 6,000) that _validate_load_test asserts.
        self.spark.range(1, 101).select(
            F.col("id").cast(IntegerType()).alias("product_id"),
            F.concat(F.lit("Product "), F.col("id")).alias("name"),
            F.lit(25.0).cast(FloatType()).alias("unit_price"),
            ((F.col("id") - 1) % 10 + 1).cast(IntegerType()).alias("category_id"),
            F.concat(F.lit("Category "), ((F.col("id") - 1) % 10 + 1).cast("string")).alias("category_name"),
        ).write.saveAsTable(f"{catalog}.{SCHEMA}.product")

        # 2M orders — customer_id round-robins through 1–500
        self.spark.range(1, 2_000_001).select(
            F.col("id").cast(IntegerType()),
            ((F.col("id") - 1) % 500 + 1).cast(IntegerType()).alias("id_customer"),
            F.lit(100.0).cast(FloatType()).alias("total"),
            F.lit("2024-01-01").alias("date"),
            (F.col("id") % 100 + 1).cast(IntegerType()).alias("product_id"),
        ).write.saveAsTable(f"{catalog}.{SCHEMA}.order")

        # 6M order_items — 3 items per order, covering all 2M orders
        self.spark.range(1, 6_000_001).select(
            (F.floor((F.col("id") - 1) / 3) + 1).cast(IntegerType()).alias("id_order"),
            ((F.col("id") - 1) % 3 + 1).cast(IntegerType()).alias("seq"),
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

        self.cluster_by(f"{catalog}.{SCHEMA}.order", "date")
        self.cluster_by(f"{catalog}.{SCHEMA}.order_item", "id_order")
        self.cluster_by(f"{catalog}.{SCHEMA}.product", "product_id")
