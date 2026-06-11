from datetime import date

from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual

from template.baseTask import BaseTask
from template.commonSchemas import order_agg_schema


class Validate(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def _validate_standard(self, catalog):
        # groupBy(customer_name, country, order_date, product_id, product_name,
        # product_category_id, category_name) → still 2 rows (one per order).
        # total_value is now sum(line_revenue) = qty × unit_price-at-sale:
        #   John Doe:   2×$10 + 1×$10 = $30  (product 1 @ $10)
        #   Jane Smith: 3×$20       = $60  (product 2 @ $20)
        expected_data = [
            ("John Doe", "USA", date(2023, 1, 1), 1, "Product 1", 1, "Category 1", 3, 30.0, 1),
            ("Jane Smith", "UK", date(2023, 1, 2), 2, "Product 2", 2, "Category 2", 3, 60.0, 1),
        ]
        df_expected = self.spark.createDataFrame(expected_data, schema=order_agg_schema)

        for table in (f"{catalog}.report.order_agg", f"{catalog}.report.order_agg_sdp"):
            df_out = self.spark.table(table)
            count = df_out.count()
            if count != 2:
                raise RuntimeError(f"Expected 2 rows in {table}, got {count}")
            assertDataFrameEqual(df_out, df_expected)

    def _validate_load_test(self, catalog):
        # 500 customers × 100 products × 1 date = 50,000 rows
        # Each (customer, product): 40 orders × 3 items × qty=2 → total_quantity=240
        # Each (customer, product): 40 orders × 3 items × total_item=50.0 → total_value=6,000.0
        # Each (customer, product): 40 distinct orders → total_orders=40
        for table in (f"{catalog}.report.order_agg", f"{catalog}.report.order_agg_sdp"):
            df_out = self.spark.table(table)
            count = df_out.count()
            if count != 50_000:
                raise RuntimeError(f"Expected 50,000 rows in {table}, got {count}")
            wrong = df_out.filter(
                (F.col("total_quantity") != 240) | (F.col("total_value") != 6_000.0) | (F.col("total_orders") != 40)
            ).count()
            if wrong > 0:
                raise RuntimeError(f"{wrong} rows in {table} have unexpected total_quantity/total_value/total_orders")

    def run(self):
        load_test = self.config.get_value("load_test") == "true"
        self.logger.info("validating integration tests (load_test=%s)", load_test)

        catalog = self.config.get_value("catalog")

        if load_test:
            self._validate_load_test(catalog)
        else:
            self._validate_standard(catalog)
