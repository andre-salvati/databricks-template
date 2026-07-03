from pyspark.sql import functions as F

from ..baseTask import BaseTask


class GenerateOrdersAgg(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def aggregate_orders(self, df_order):
        # total_value sums item_total — the line value the source froze on the order at
        # sale time, so a later price change never restates historical revenue.
        return df_order.groupBy(
            "customer_name",
            "country",
            "order_date",
            "product_id",
            "product_name",
            "product_category_id",
            "category_name",
        ).agg(
            F.sum("item_quantity").alias("total_quantity"),
            F.sum("item_total").alias("total_value"),
            F.countDistinct("order_id").alias("total_orders"),
        )

    def run(self):
        self.logger.info("generating orders aggregated")
        seed_date = self.config.get_value("seed_date")

        first_run = (
            not self.spark.catalog.tableExists("report.order_agg") or self.spark.table("report.order_agg").isEmpty()
        )

        if first_run:
            # Backfill: aggregate the whole silver table once.
            df_out = self.aggregate_orders(self.spark.read.table("curated.order_enriched"))
            (df_out.write.mode("overwrite").option("overwriteSchema", "false").saveAsTable("report.order_agg"))
        else:
            # Daily incremental: aggregate only seed_date's silver slice (clustering by
            # order_date prunes the scan) and atomically replace that date's rows.
            # Historical dates stay untouched.
            df_slice = self.spark.read.table("curated.order_enriched").filter(F.col("order_date") == seed_date)
            df_out = self.aggregate_orders(df_slice)
            (
                df_out.write.mode("overwrite")
                .option("replaceWhere", f"order_date = DATE'{seed_date}'")
                .option("overwriteSchema", "false")
                .saveAsTable("report.order_agg")
            )

        self.cluster_by("report.order_agg", "order_date", "product_id")
