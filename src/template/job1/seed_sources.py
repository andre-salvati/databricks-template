from datetime import date as _date

from pyspark.sql import DataFrame

from ..baseTask import BaseTask
from ..commonSchemas import customer_schema, order_item_schema, order_schema

SCHEMA = "external_source"

# Synthetic ID space starts at 1_000 and grows by 10 per day so there is no
# overlap with integration-test rows (customer ids 10/20, order ids 1-3).
_EPOCH = _date(2024, 1, 1)


def _day_offset(seed_date: str) -> int:
    """Days since _EPOCH — deterministic, collision-free index per calendar day."""
    return max(0, (_date.fromisoformat(seed_date) - _EPOCH).days)


def _base_id(seed_date: str) -> int:
    """First synthetic primary-key value available for seed_date (10 IDs reserved/day)."""
    return _day_offset(seed_date) * 10 + 1_000


class SeedSources(BaseTask):
    """
    Idempotent daily seeder for external_source tables.

    On first run: creates each table from its declared schema if it does not
    exist yet (``mode="ignore"`` is a no-op when the table is already present).
    On every run: appends one day's worth of synthetic rows keyed by seed_date,
    using MERGE-by-primary-key so reruns of the same day are safe no-ops.

    In a real pipeline, replace the ``_generate_*`` methods with reads from
    your actual upstream source (S3 landing zone, JDBC connection, REST API,
    Kafka topic, etc.).  The ``_upsert_*`` helpers stay unchanged.
    """

    def run(self) -> None:
        catalog = self.config.get_value("catalog")
        seed_date = self.config.get_value("seed_date")
        self.logger.info("seeding external_source tables catalog=%s date=%s", catalog, seed_date)

        self._ensure_tables(catalog)

        n_customers = self._upsert_customers(catalog, seed_date)
        n_orders, n_items = self._upsert_orders(catalog, seed_date)

        self.logger.info(
            "seed complete customers_upserted=%d orders_upserted=%d items_upserted=%d",
            n_customers,
            n_orders,
            n_items,
        )

    # ------------------------------------------------------------------
    # Table bootstrap (idempotent CREATE IF NOT EXISTS)
    # ------------------------------------------------------------------

    def _ensure_tables(self, catalog: str) -> None:
        """Create each external_source table from its declared schema when absent."""
        for name, schema in (
            ("customer", customer_schema),
            ("order", order_schema),
            ("order_item", order_item_schema),
        ):
            (self.spark.createDataFrame([], schema).write.mode("ignore").saveAsTable(f"{catalog}.{SCHEMA}.{name}"))
            self.logger.info("ensured table %s.%s.%s", catalog, SCHEMA, name)

    # ------------------------------------------------------------------
    # Synthetic row generators
    # Replace with real upstream reads in production.
    # ------------------------------------------------------------------

    def _generate_customers(self, seed_date: str) -> DataFrame:
        """One new customer per day; id derived from seed_date so there are no cross-day collisions."""
        base = _base_id(seed_date)
        rows = [(base, f"Customer_{base}", "US")]
        return self.spark.createDataFrame(rows, schema=customer_schema)

    def _generate_orders(self, seed_date: str) -> DataFrame:
        """Two new orders per day, both belonging to today's customer."""
        base = _base_id(seed_date)
        rows = [
            (base, base, 99.9, seed_date),
            (base + 1, base, 49.5, seed_date),
        ]
        return self.spark.createDataFrame(rows, schema=order_schema)

    def _generate_order_items(self, seed_date: str) -> DataFrame:
        """Three line items: two for the first order of the day, one for the second."""
        base = _base_id(seed_date)
        rows = [
            (base, 1, "Widget A", 2, 49.95),
            (base, 2, "Widget B", 1, 49.95),
            (base + 1, 1, "Widget C", 1, 49.5),
        ]
        return self.spark.createDataFrame(rows, schema=order_item_schema)

    # ------------------------------------------------------------------
    # Upsert helpers — MERGE by PK so reruns on the same day are no-ops
    # ------------------------------------------------------------------

    def _upsert_customers(self, catalog: str, seed_date: str) -> int:
        df = self._generate_customers(seed_date)
        df.createOrReplaceTempView("_seed_customers")
        self.spark.sql(f"""
            MERGE INTO {catalog}.{SCHEMA}.customer AS t
            USING _seed_customers AS s ON t.id = s.id
            WHEN NOT MATCHED THEN INSERT *
        """)
        return df.count()

    def _upsert_orders(self, catalog: str, seed_date: str) -> tuple[int, int]:
        df_orders = self._generate_orders(seed_date)
        df_items = self._generate_order_items(seed_date)
        df_orders.createOrReplaceTempView("_seed_orders")
        df_items.createOrReplaceTempView("_seed_order_items")
        self.spark.sql(f"""
            MERGE INTO {catalog}.{SCHEMA}.order AS t
            USING _seed_orders AS s ON t.id = s.id
            WHEN NOT MATCHED THEN INSERT *
        """)
        self.spark.sql(f"""
            MERGE INTO {catalog}.{SCHEMA}.order_item AS t
            USING _seed_order_items AS s ON t.id_order = s.id_order AND t.seq = s.seq
            WHEN NOT MATCHED THEN INSERT *
        """)
        return df_orders.count(), df_items.count()
