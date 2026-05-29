from datetime import date as _date

from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType

from ..baseTask import BaseTask
from ..commonSchemas import customer_schema, order_item_schema, order_schema

SCHEMA = "external_source"

_EPOCH = _date(2024, 1, 1)
_COUNTRIES = ["US", "UK", "CA", "DE", "FR"]

_INITIAL_CUSTOMERS = 500
_INITIAL_ORDERS = 2_000_000
_INITIAL_ITEMS = 6_000_000  # 3 per order

_INCREMENTAL_ORDERS = 2_000
_INCREMENTAL_CUSTOMER_UPDATES = 50


class SeedSources(BaseTask):
    """
    Idempotent seeder for external_source tables.

    First run (empty tables): full initial load — 500 customers, 2M orders, 6M order_items.
    Subsequent runs: append 2000 orders (+1 item each) and update 50 customers' country.
    Incremental IDs are anchored to seed_date so reruns of the same date are no-ops.
    """

    def run(self) -> None:
        catalog = self.config.get_value("catalog")
        seed_date = self.config.get_value("seed_date")

        self._ensure_tables(catalog)

        count = self.spark.table(f"{catalog}.{SCHEMA}.customer").count()
        if count == 0:
            self.logger.info(
                "initial load: %d customers, %d orders, %d items",
                _INITIAL_CUSTOMERS,
                _INITIAL_ORDERS,
                _INITIAL_ITEMS,
            )
            self._seed_initial(catalog)
            self.logger.info("initial load complete")
        else:
            self.logger.info("incremental seed date=%s", seed_date)
            self._seed_incremental(catalog, seed_date)

    # ------------------------------------------------------------------
    # Table bootstrap
    # ------------------------------------------------------------------

    def _ensure_tables(self, catalog: str) -> None:
        for name, schema in (
            ("customer", customer_schema),
            ("order", order_schema),
            ("order_item", order_item_schema),
        ):
            self.spark.createDataFrame([], schema).write.mode("ignore").saveAsTable(f"{catalog}.{SCHEMA}.{name}")

    # ------------------------------------------------------------------
    # Initial load (first run only)
    # ------------------------------------------------------------------

    def _seed_initial(self, catalog: str) -> None:
        self.spark.range(1, _INITIAL_CUSTOMERS + 1).select(
            F.col("id").cast(IntegerType()),
            F.concat(F.lit("Customer_"), F.col("id")).alias("name"),
            F.lit("US").alias("country"),
        ).write.mode("overwrite").option("overwriteSchema", "false").saveAsTable(f"{catalog}.{SCHEMA}.customer")

        self.spark.range(1, _INITIAL_ORDERS + 1).select(
            F.col("id").cast(IntegerType()),
            ((F.col("id") - 1) % _INITIAL_CUSTOMERS + 1).cast(IntegerType()).alias("id_customer"),
            F.lit(100.0).cast(FloatType()).alias("total"),
            F.lit("2024-01-01").alias("date"),
        ).write.mode("overwrite").option("overwriteSchema", "false").saveAsTable(f"{catalog}.{SCHEMA}.order")

        self.spark.range(1, _INITIAL_ITEMS + 1).select(
            (F.floor((F.col("id") - 1) / 3) + 1).cast(IntegerType()).alias("id_order"),
            ((F.col("id") - 1) % 3 + 1).cast(IntegerType()).alias("seq"),
            F.concat(F.lit("Item_"), F.col("id")).alias("desc_item"),
            F.lit(2).cast(IntegerType()).alias("qty"),
            F.lit(50.0).cast(FloatType()).alias("total_item"),
        ).write.mode("overwrite").option("overwriteSchema", "false").saveAsTable(f"{catalog}.{SCHEMA}.order_item")

    # ------------------------------------------------------------------
    # Incremental (every subsequent run)
    # ------------------------------------------------------------------

    def _seed_incremental(self, catalog: str, seed_date: str) -> None:
        day_offset = (_date.fromisoformat(seed_date) - _EPOCH).days
        order_base = _INITIAL_ORDERS + day_offset * _INCREMENTAL_ORDERS

        # 2000 new orders
        self.spark.range(order_base + 1, order_base + _INCREMENTAL_ORDERS + 1).select(
            F.col("id").cast(IntegerType()),
            ((F.col("id") - 1) % _INITIAL_CUSTOMERS + 1).cast(IntegerType()).alias("id_customer"),
            F.lit(100.0).cast(FloatType()).alias("total"),
            F.lit(seed_date).alias("date"),
        ).createOrReplaceTempView("_seed_incr_orders")
        self.spark.sql(f"""
            MERGE INTO {catalog}.{SCHEMA}.order AS t
            USING _seed_incr_orders AS s ON t.id = s.id
            WHEN NOT MATCHED THEN INSERT *
        """)

        # 1 item per new order
        self.spark.range(order_base + 1, order_base + _INCREMENTAL_ORDERS + 1).select(
            F.col("id").cast(IntegerType()).alias("id_order"),
            F.lit(1).cast(IntegerType()).alias("seq"),
            F.concat(F.lit("Item_incr_"), F.col("id")).alias("desc_item"),
            F.lit(2).cast(IntegerType()).alias("qty"),
            F.lit(50.0).cast(FloatType()).alias("total_item"),
        ).createOrReplaceTempView("_seed_incr_items")
        self.spark.sql(f"""
            MERGE INTO {catalog}.{SCHEMA}.order_item AS t
            USING _seed_incr_items AS s ON t.id_order = s.id_order AND t.seq = s.seq
            WHEN NOT MATCHED THEN INSERT *
        """)

        # Update country for 50 customers, cycling through all 500 every 10 days
        start = day_offset * _INCREMENTAL_CUSTOMER_UPDATES % _INITIAL_CUSTOMERS
        customer_ids = [((start + i) % _INITIAL_CUSTOMERS) + 1 for i in range(_INCREMENTAL_CUSTOMER_UPDATES)]
        new_country = _COUNTRIES[day_offset % len(_COUNTRIES)]
        update_rows = [(cid, f"Customer_{cid}", new_country) for cid in customer_ids]
        self.spark.createDataFrame(update_rows, schema=customer_schema).createOrReplaceTempView(
            "_seed_customer_updates"
        )
        self.spark.sql(f"""
            MERGE INTO {catalog}.{SCHEMA}.customer AS t
            USING _seed_customer_updates AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET t.country = s.country
        """)

        self.logger.info(
            "incremental seed complete date=%s orders=%d items=%d customers_updated=%d country=%s",
            seed_date,
            _INCREMENTAL_ORDERS,
            _INCREMENTAL_ORDERS,
            _INCREMENTAL_CUSTOMER_UPDATES,
            new_country,
        )
