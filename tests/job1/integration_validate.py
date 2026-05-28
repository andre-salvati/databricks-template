from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from template.baseTask import BaseTask


class Validate(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def run(self):
        self.logger.info("validating integration tests")

        catalog = self.config.get_value("catalog")

        expected_data = [
            ("John Doe", 3, 100.0),
            ("Jane Smith", 3, 151.0),
        ]
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
