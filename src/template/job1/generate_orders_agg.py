from pyspark.sql.functions import countDistinct, sum

from ..baseTask import BaseTask


class GenerateOrdersAgg(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def aggregate_orders(self, df_order):
        # TODO code your transformations here...

        return df_order.groupBy("customer_name", "country", "order_date", "product_id", "product_category_id").agg(
            sum("item_quantity").alias("total_quantity"),
            sum("item_total").alias("total_value"),
            countDistinct("order_id").alias("total_orders"),
        )

    def run(self):
        self.logger.info("generating orders aggregated")

        df_order = self.spark.read.table("curated.order_enriched")

        df_out = self.aggregate_orders(df_order)

        (df_out.write.mode("overwrite").option("overwriteSchema", "false").saveAsTable("report.order_agg"))
