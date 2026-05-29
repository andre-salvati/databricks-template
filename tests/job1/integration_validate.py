from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from template.baseTask import BaseTask


class Validate(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def _validate_standard(self, catalog):
        expected_data = [("John Doe", 3, 100.0), ("Jane Smith", 3, 151.0)]
        expected_schema = StructType(
            [
                StructField("name", StringType(), True),
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
        # 500 customers × 4000 orders each × 3 items × qty=2, total_item=50.0
        # → total_qty=24_000 and total_value=600_000.0 per customer
        for table in (f"{catalog}.report.order_agg", f"{catalog}.report.order_agg_sdp"):
            df_out = self.spark.table(table)
            count = df_out.count()
            if count != 500:
                raise RuntimeError(f"Expected 500 rows in {table}, got {count}")
            wrong = df_out.filter((F.col("total_qty") != 24_000) | (F.col("total_value") != 600_000.0)).count()
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
