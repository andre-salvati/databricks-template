from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from template.baseTask import BaseTask

# Customers seeded by integration_setup.py with deterministic, known values.
# seed_sources adds synthetic rows on top of these; we filter to setup customers
# so the assertion is stable regardless of which seed_date was used.
_SETUP_CUSTOMERS = ["John Doe", "Jane Smith"]


class Validate(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def run(self):
        self.logger.info("validating integration tests")

        df_out = self.spark.table("report.order_agg")

        # Filter to the two customers seeded by setup so the check is not
        # sensitive to extra rows introduced by seed_sources on the same run.
        df_setup = df_out.filter(col("name").isin(_SETUP_CUSTOMERS))

        count = df_setup.count()
        if count != 2:
            raise RuntimeError(f"Expected 2 rows for setup customers in report.order_agg, got {count}")

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

        assertDataFrameEqual(df_setup, df_expected)
