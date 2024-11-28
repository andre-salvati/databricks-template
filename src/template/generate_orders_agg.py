from funcy import print_durations

from .baseTask import BaseTask

from pyspark.sql.functions import sum

schema = "report"


class GenerateOrdersAgg(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    @print_durations
    def aggregate_orders(self, df_order):
        
        # TODO code your transformations here...

        return (df_order.groupBy("name")
                    .agg(sum("qty").alias("total_qty"), sum("total_item").alias("total_value")))
    
    def run(self):
        print("Generating Orders Aggregated ...")

        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        df_order = self.spark.read.table("curated.order_enriched")

        df_out = self.aggregate_orders(df_order)

        if self.config.get_value("debug"):
            df_out.show()

        df_out.write.mode("overwrite").saveAsTable(f"{schema}.order_agg")
