from datetime import date as _date

from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType

from ..baseTask import BaseTask
from ..commonSchemas import customer_schema, order_item_schema, order_schema, product_schema

SCHEMA = "external_source"

_EPOCH = _date(2024, 1, 1)
_COUNTRIES = ["US", "UK", "CA", "DE", "FR", "BR", "AU", "JP", "MX", "IN"]

_INITIAL_CUSTOMERS = 500
_INITIAL_PRODUCTS = 100  # product_id range matches order.product_id = id % 100 + 1
_INITIAL_ORDERS = 2_000_000
_INITIAL_ITEMS = 6_000_000  # 3 per order

_INCREMENTAL_ORDERS = 5_000
_INCREMENTAL_CUSTOMER_UPDATES = 50
_INCREMENTAL_PRICE_UPDATES = 5  # products whose unit_price changes each day


class SeedSources(BaseTask):
    """
    Idempotent seeder for external_source tables.

    First run (empty tables): full initial load — 500 customers, 2M orders, 6M order_items.
    Subsequent runs: append 5 000 orders (+1 item each) and update 50 customers' country.
    Incremental IDs are anchored to seed_date so reruns of the same date are no-ops.
    """

    def run(self) -> None:
        catalog = self.config.get_value("catalog")
        seed_date = self.config.get_value("seed_date")

        self._ensure_tables(catalog)

        count = self.spark.table(f"{catalog}.{SCHEMA}.customer").count()
        if count < _INITIAL_CUSTOMERS:
            self.logger.info(
                "initial load: %d customers, %d orders, %d items",
                _INITIAL_CUSTOMERS,
                _INITIAL_ORDERS,
                _INITIAL_ITEMS,
            )
            self._seed_initial(catalog, seed_date)
            self.logger.info("initial load complete")
        else:
            self.logger.info("incremental seed date=%s", seed_date)
            self._seed_incremental(catalog, seed_date)

        self.logger.info(
            "external_source totals: customers=%d orders=%d order_items=%d",
            self.spark.table(f"{catalog}.{SCHEMA}.customer").count(),
            self.spark.table(f"{catalog}.{SCHEMA}.order").count(),
            self.spark.table(f"{catalog}.{SCHEMA}.order_item").count(),
        )

    # ------------------------------------------------------------------
    # Table bootstrap
    # ------------------------------------------------------------------

    def _ensure_tables(self, catalog: str) -> None:
        for name, schema in (
            ("customer", customer_schema),
            ("product", product_schema),
            ("order", order_schema),
            ("order_item", order_item_schema),
        ):
            self.spark.createDataFrame([], schema).write.mode("ignore").saveAsTable(f"{catalog}.{SCHEMA}.{name}")

        # Liquid clustering on the accumulating source tables. order grows by date
        # (append), order_item by id_order (append), product by product_id (MERGE).
        self.cluster_by(f"{catalog}.{SCHEMA}.order", "date")
        self.cluster_by(f"{catalog}.{SCHEMA}.order_item", "id_order")
        self.cluster_by(f"{catalog}.{SCHEMA}.product", "product_id")

    # ------------------------------------------------------------------
    # Initial load (first run only)
    # ------------------------------------------------------------------

    def _seed_initial(self, catalog: str, seed_date: str) -> None:
        # Non-uniform country distribution so country chart lines are clearly separated.
        # US=200, UK=100, DE=50, FR=50, BR=30, CA=25, AU=20, JP=15, MX=7, IN=3 (total=500)
        self.spark.range(1, _INITIAL_CUSTOMERS + 1).select(
            F.col("id").cast(IntegerType()),
            F.concat(F.lit("Customer_"), F.col("id")).alias("name"),
            F.when(F.col("id") <= 200, "US")
            .when(F.col("id") <= 300, "UK")
            .when(F.col("id") <= 350, "DE")
            .when(F.col("id") <= 400, "FR")
            .when(F.col("id") <= 430, "BR")
            .when(F.col("id") <= 455, "CA")
            .when(F.col("id") <= 475, "AU")
            .when(F.col("id") <= 490, "JP")
            .when(F.col("id") <= 497, "MX")
            .otherwise("IN")
            .alias("country"),
        ).write.mode("overwrite").option("overwriteSchema", "false").saveAsTable(f"{catalog}.{SCHEMA}.customer")

        # Product dimension: 100 products. unit_price starts category-banded
        # (category = (product_id-1) % 10 + 1, so price spreads $10–$55) and is bumped
        # over time by the incremental seed — that mutation is what the silver freeze
        # vs. restate behaviour is demonstrated against. name is the readable label.
        self.spark.range(1, _INITIAL_PRODUCTS + 1).select(
            F.col("id").cast(IntegerType()).alias("product_id"),
            F.concat(F.lit("Product "), F.col("id")).alias("name"),
            (((F.col("id") - 1) % 10 + 1) * 5.0 + 4.99).cast(FloatType()).alias("unit_price"),
            ((F.col("id") - 1) % 10 + 1).cast(IntegerType()).alias("category_id"),
            F.concat(F.lit("Category "), ((F.col("id") - 1) % 10 + 1).cast("string")).alias("category_name"),
        ).write.mode("overwrite").option("overwriteSchema", "false").saveAsTable(f"{catalog}.{SCHEMA}.product")

        self.spark.range(1, _INITIAL_ORDERS + 1).select(
            F.col("id").cast(IntegerType()),
            ((F.col("id") - 1) % _INITIAL_CUSTOMERS + 1).cast(IntegerType()).alias("id_customer"),
            ((F.col("id") % 99 + 1) * 10).cast(FloatType()).alias("total"),
            F.date_sub(F.lit(seed_date), (F.col("id") % 363).cast(IntegerType())).cast("string").alias("date"),
            (F.col("id") % 100 + 1).cast(IntegerType()).alias("product_id"),
        ).write.mode("overwrite").option("overwriteSchema", "false").saveAsTable(f"{catalog}.{SCHEMA}.order")

        # total_item scales with category (category_id * $15 base + $10 noise) so category 1
        # items cost ~$25 and category 10 items cost ~$160 — a 6x spread visible in charts.
        # category_id is derived from id_order using the same formula as the order table.
        self.spark.range(1, _INITIAL_ITEMS + 1).select(
            (F.floor((F.col("id") - 1) / 3) + 1).cast(IntegerType()).alias("id_order"),
            ((F.col("id") - 1) % 3 + 1).cast(IntegerType()).alias("seq"),
            F.concat(F.lit("Item_"), F.col("id")).alias("desc_item"),
            (F.floor(F.rand(seed=7) * 5) + 1).cast(IntegerType()).alias("qty"),
            F.round(
                ((F.floor((F.col("id") - 1) / 3) + 1) % 10 + 1).cast(FloatType()) * 15.0 + F.rand(seed=42) * 10.0,
                2,
            )
            .cast(FloatType())
            .alias("total_item"),
        ).write.mode("overwrite").option("overwriteSchema", "false").saveAsTable(f"{catalog}.{SCHEMA}.order_item")

    # ------------------------------------------------------------------
    # Incremental (every subsequent run)
    # ------------------------------------------------------------------

    def _seed_incremental(self, catalog: str, seed_date: str) -> None:
        self._build_incremental_orders(seed_date).write.mode("append").option("overwriteSchema", "false").saveAsTable(
            f"{catalog}.{SCHEMA}.order"
        )

        self._build_incremental_items(seed_date).write.mode("append").option("overwriteSchema", "false").saveAsTable(
            f"{catalog}.{SCHEMA}.order_item"
        )

        df_customers = self._build_customer_updates(seed_date)
        df_customers.createOrReplaceTempView("_seed_customer_updates")
        self.spark.sql(f"""
            MERGE INTO {catalog}.{SCHEMA}.customer AS t
            USING _seed_customer_updates AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET t.country = s.country
        """)

        df_prices = self._build_price_updates(seed_date)
        df_prices.createOrReplaceTempView("_seed_price_updates")
        self.spark.sql(f"""
            MERGE INTO {catalog}.{SCHEMA}.product AS t
            USING _seed_price_updates AS s ON t.product_id = s.product_id
            WHEN MATCHED THEN UPDATE SET t.unit_price = s.unit_price
        """)

        day_offset = (_date.fromisoformat(seed_date) - _EPOCH).days
        self.logger.info(
            "incremental seed complete date=%s orders=%d items=%d customers_updated=%d prices_updated=%d country=%s",
            seed_date,
            _INCREMENTAL_ORDERS,
            _INCREMENTAL_ORDERS,
            _INCREMENTAL_CUSTOMER_UPDATES,
            _INCREMENTAL_PRICE_UPDATES,
            _COUNTRIES[day_offset % len(_COUNTRIES)],
        )

    def _build_incremental_orders(self, seed_date: str):
        day_offset = (_date.fromisoformat(seed_date) - _EPOCH).days
        order_base = _INITIAL_ORDERS + day_offset * _INCREMENTAL_ORDERS
        return self.spark.range(order_base + 1, order_base + _INCREMENTAL_ORDERS + 1).select(
            F.col("id").cast(IntegerType()),
            ((F.col("id") - 1) % _INITIAL_CUSTOMERS + 1).cast(IntegerType()).alias("id_customer"),
            ((F.col("id") % 99 + 1) * 10).cast(FloatType()).alias("total"),
            F.lit(seed_date).alias("date"),
            (F.col("id") % 100 + 1).cast(IntegerType()).alias("product_id"),
        )

    def _build_incremental_items(self, seed_date: str):
        day_offset = (_date.fromisoformat(seed_date) - _EPOCH).days
        order_base = _INITIAL_ORDERS + day_offset * _INCREMENTAL_ORDERS
        return self.spark.range(order_base + 1, order_base + _INCREMENTAL_ORDERS + 1).select(
            F.col("id").cast(IntegerType()).alias("id_order"),
            F.lit(1).cast(IntegerType()).alias("seq"),
            F.concat(F.lit("Item_incr_"), F.col("id")).alias("desc_item"),
            (F.floor(F.rand(seed=7) * 5) + 1).cast(IntegerType()).alias("qty"),
            F.round(
                (F.col("id") % 10 + 1).cast(FloatType()) * 15.0 + F.rand(seed=42) * 10.0,
                2,
            )
            .cast(FloatType())
            .alias("total_item"),
        )

    def _build_customer_updates(self, seed_date: str):
        day_offset = (_date.fromisoformat(seed_date) - _EPOCH).days
        start = day_offset * _INCREMENTAL_CUSTOMER_UPDATES % _INITIAL_CUSTOMERS
        customer_ids = [((start + i) % _INITIAL_CUSTOMERS) + 1 for i in range(_INCREMENTAL_CUSTOMER_UPDATES)]
        new_country = _COUNTRIES[day_offset % len(_COUNTRIES)]
        update_rows = [(cid, f"Customer_{cid}", new_country) for cid in customer_ids]
        return self.spark.createDataFrame(update_rows, schema=customer_schema)

    def _build_price_updates(self, seed_date: str):
        # Bump 5 products' unit_price each day by a day-anchored factor. Anchoring the
        # factor to seed_date keeps reruns of the same date idempotent (same result).
        day_offset = (_date.fromisoformat(seed_date) - _EPOCH).days
        start = day_offset * _INCREMENTAL_PRICE_UPDATES % _INITIAL_PRODUCTS
        product_ids = [((start + i) % _INITIAL_PRODUCTS) + 1 for i in range(_INCREMENTAL_PRICE_UPDATES)]
        factor = 1.0 + ((day_offset % 5) - 2) * 0.05  # -10% .. +10%
        update_rows = [
            (
                pid,
                f"Product {pid}",
                round(((pid - 1) % 10 + 1) * 5.0 + 4.99) * factor,
                (pid - 1) % 10 + 1,
                f"Category {(pid - 1) % 10 + 1}",
            )
            for pid in product_ids
        ]
        return self.spark.createDataFrame(update_rows, schema=product_schema)
