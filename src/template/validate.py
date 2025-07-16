from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from .baseTask import BaseTask

schema = "report"


class Validate(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def run(self):
        print("Validating integration tests ...")

        df_out = self.spark.table("report.order_agg")

        assert df_out.count() == 2

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

        assertDataFrameEqual(df_out, df_expected)
