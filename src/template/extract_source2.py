from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from .baseTask import BaseTask
from .commonSchemas import order_schema, order_item_schema

schema = "raw_source2"


class ExtractSource2(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def run(self):
        print("Extracting data from Source2 ...")

        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        order_data = [(1, 10, 100.0, "2023-01-01"), (2, 20, 150.0, "2023-01-02")]
        df_order = self.spark.createDataFrame(order_data, schema=order_schema)

        order_item_data = [(1, 1, "Item A", 2, 50.0), (1, 2, "Item B", 1, 50.0), (2, 1, "Item C", 3, 150.0)]
        df_order_item = self.spark.createDataFrame(order_item_data, schema=order_item_schema)

        if self.config.get_value("debug"):
            df_order.show()
            df_order_item.show()

        df_order.write.mode("overwrite").saveAsTable(f"{schema}.order")
        df_order_item.write.mode("overwrite").saveAsTable(f"{schema}.order_item")
