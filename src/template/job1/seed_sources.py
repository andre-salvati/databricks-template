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
_INCREMENTAL_NAME_UPDATES = 2  # products whose name changes each day


def _product_category(product_id: int) -> tuple[int, str]:
    """Stable category attribute of a product, mirroring the Spark expression in
    _seed_initial: category = (product_id - 1) % 10 + 1. Used to build the name-update
    rows so the formula lives in one place and the two product-writing paths can't drift."""
    cat = (product_id - 1) % 10 + 1
    return cat, f"Category {cat}"


def _product_unit_price(product_id: int) -> float:
    """Static unit_price of a product, mirroring the Spark expression in _seed_initial.
    unit_price never changes after the initial load; it is carried on name-update rows
    only to satisfy product_schema (the MERGE updates name alone)."""
    return float(((product_id - 1) % 10 + 1) * 5.0 + 4.99)


class SeedSources(BaseTask):
    """
    Idempotent seeder for external_source tables.

    First run (empty tables): full initial load — 500 customers, 2M orders, 6M order_items.
    Subsequent runs: append 5 000 orders (+1 item each), update 50 customers' country, and
    rename 2 products (e.g. "Product 1" → "Product 1.1"). Incremental IDs and renames are
    anchored to seed_date so reruns of the same date are no-ops.
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

        # Product dimension: 100 products. unit_price is category-banded
        # (category = (product_id-1) % 10 + 1, so price spreads $10–$55) and STATIC — it
        # never changes after this load. name ("Product N") is the mutable attribute: the
        # incremental seed renames a couple of products each day, and that mutation is what
        # the silver freeze vs. restate behaviour is demonstrated against.
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

        df_names = self._build_name_updates(seed_date)
        df_names.createOrReplaceTempView("_seed_name_updates")
        self.spark.sql(f"""
            MERGE INTO {catalog}.{SCHEMA}.product AS t
            USING _seed_name_updates AS s ON t.product_id = s.product_id
            WHEN MATCHED THEN UPDATE SET t.name = s.name
        """)

        day_offset = (_date.fromisoformat(seed_date) - _EPOCH).days
        self.logger.info(
            "incremental seed complete date=%s orders=%d items=%d customers_updated=%d names_updated=%d country=%s",
            seed_date,
            _INCREMENTAL_ORDERS,
            _INCREMENTAL_ORDERS,
            _INCREMENTAL_CUSTOMER_UPDATES,
            _INCREMENTAL_NAME_UPDATES,
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

    @staticmethod
    def _products_renamed_on(day_offset: int) -> list[int]:
        """The product ids selected for a rename on a given day, by a day-anchored
        rotation. Deterministic so reruns of the same date are idempotent."""
        start = day_offset * _INCREMENTAL_NAME_UPDATES % _INITIAL_PRODUCTS
        return [((start + i) % _INITIAL_PRODUCTS) + 1 for i in range(_INCREMENTAL_NAME_UPDATES)]

    def _build_name_updates(self, seed_date: str):
        # Rename 2 products each day: "Product N" → "Product N.k", where k is the
        # cumulative number of times product N has been selected through this day. The
        # suffix increments on each successive rename (Product 1.1, Product 1.2, ...).
        # Everything is a deterministic function of the day offset, so reruns of the same
        # date produce identical names (idempotent MERGE).
        day_offset = (_date.fromisoformat(seed_date) - _EPOCH).days
        # The MERGE below only updates name; unit_price/category columns are carried solely
        # to satisfy product_schema and derived from the shared helpers so they stay correct
        # if the MERGE is ever extended.
        update_rows = []
        for pid in self._products_renamed_on(day_offset):
            k = sum(1 for d in range(day_offset + 1) if pid in self._products_renamed_on(d))
            update_rows.append((pid, f"Product {pid}.{k}", _product_unit_price(pid), *_product_category(pid)))
        return self.spark.createDataFrame(update_rows, schema=product_schema)
