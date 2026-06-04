from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from template.baseTask import BaseTask


class Validate(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def _validate_standard(self, catalog):
        # groupBy(name, country, date, product_id, prod_category_id) → still 2 rows (one per order)
        expected_data = [
            ("John Doe", "USA", "2023-01-01", 1, 1, 3, 100.0),
            ("Jane Smith", "UK", "2023-01-02", 2, 1, 3, 151.0),
        ]
        expected_schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("date", StringType(), True),
                StructField("product_id", IntegerType(), True),
                StructField("prod_category_id", IntegerType(), True),
                StructField("total_qty", LongType(), True),
                StructField("total_value", DoubleType(), True),
            ]
        )
        df_expected = self.spark.createDataFrame(expected_data, schema=expected_schema)

        for table in (f"{catalog}.report.order_agg", f"{catalog}.report.order_agg_sdp"):
            df_out = self.spark.table(table)
            count = df_out.count()
            if count != 2:
                raise RuntimeError(f"Expected 2 rows in {table}, got {count}")
            assertDataFrameEqual(df_out, df_expected)

    def _validate_load_test(self, catalog):
        # 500 customers × 100 products × 1 date = 50,000 rows
        # Each (customer, product): 40 orders × 3 items × qty=2 → total_qty=240
        # Each (customer, product): 40 orders × 3 items × total_item=50.0 → total_value=6,000.0
        for table in (f"{catalog}.report.order_agg", f"{catalog}.report.order_agg_sdp"):
            df_out = self.spark.table(table)
            count = df_out.count()
            if count != 50_000:
                raise RuntimeError(f"Expected 50,000 rows in {table}, got {count}")
            wrong = df_out.filter((F.col("total_qty") != 240) | (F.col("total_value") != 6_000.0)).count()
            if wrong > 0:
                raise RuntimeError(f"{wrong} rows in {table} have unexpected total_qty/total_value")

    def run(self):
        load_test = self.config.get_value("load_test") == "true"
        self.logger.info("validating integration tests (load_test=%s)", load_test)

        catalog = self.config.get_value("catalog")

        if load_test:
            self._validate_load_test(catalog)
        else:
            self._validate_standard(catalog)
