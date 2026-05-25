from ..baseTask import BaseTask
from ..commonSchemas import customer_schema, order_item_schema, order_schema

SCHEMA = "external_source"


class Setup(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def run(self):
        self.logger.info("setup for integration tests")

        # Destructive DDL: qualify with the catalog so it's unmistakably scoped to the
        # current env's catalog, never the workspace default.
        catalog = self.config.get_value("catalog")
        for s in (SCHEMA, "raw", "curated", "report"):
            self.spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{s} CASCADE")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA}")

        # customer

        customer_data = [(10, "John Doe", "USA"), (20, "Jane Smith", "UK")]
        df_customer = self.spark.createDataFrame(customer_data, schema=customer_schema)
        df_customer.write.saveAsTable(f"{catalog}.{SCHEMA}.customer")

        # order

        order_data = [
            (1, 10, 100.0, "2023-01-01"),
            (2, 20, 151.0, "2023-01-02"),
            (None, 10, 100.0, "2023-01-01"),  # id is null
            (3, 20, 150.0, "2023-01-02"),  # id is duplicated
            (3, 20, 150.0, "2023-01-02"),
        ]  # id is duplicated
        df_order = self.spark.createDataFrame(order_data, schema=order_schema)
        df_order.write.saveAsTable(f"{catalog}.{SCHEMA}.order")

        order_item_data = [(1, 1, "Item A", 2, 50.0), (1, 2, "Item B", 1, 50.0), (2, 1, "Item C", 3, 151.0)]
        df_order_item = self.spark.createDataFrame(order_item_data, schema=order_item_schema)
        df_order_item.write.saveAsTable(f"{catalog}.{SCHEMA}.order_item")
