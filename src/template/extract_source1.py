from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from .baseTask import BaseTask

schema = "raw_source1"


class ExtractSource1(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def run(self):
        print("Extracting data from Source1 ...")

        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        customer_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("country", StringType(), True),
            ]
        )

        customer_data = [(10, "John Doe", "USA"), (20, "Jane Smith", "UK")]

        df = self.spark.createDataFrame(customer_data, schema=customer_schema)

        if self.config.get_value("debug"):
            df.show()

        df.write.mode("overwrite").saveAsTable(f"{schema}.customer")
