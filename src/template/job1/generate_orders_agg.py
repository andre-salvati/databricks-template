from pyspark.sql.functions import sum

from ..baseTask import BaseTask


class GenerateOrdersAgg(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def aggregate_orders(self, df_order):
        # TODO code your transformations here...

        return df_order.groupBy("name").agg(sum("qty").alias("total_qty"), sum("total_item").alias("total_value"))

    def run(self):
        self.logger.info("generating orders aggregated")

        df_order = self.spark.read.table("curated.order_enriched")

        df_out = self.aggregate_orders(df_order)

        (df_out.write.mode("overwrite").option("overwriteSchema", "false").saveAsTable("report.order_agg"))
