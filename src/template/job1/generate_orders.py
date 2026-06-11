from pyspark.sql import functions as F

from ..baseTask import BaseTask


class GenerateOrders(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def enrich_order(self, df_customer, df_order, df_order_item, df_product):
        # order_item ⨝ order ⨝ customer ⨝ product. line_revenue is computed from the
        # product dimension's CURRENT unit_price at the moment this row is processed;
        # the incremental MERGE in run() then freezes it (INSERT-only, never updated).
        return (
            df_order_item.join(df_order, df_order_item["id_order"] == df_order["id"])
            .join(df_customer, df_order["id_customer"] == df_customer["id"])
            .join(df_product, df_order["product_id"] == df_product["product_id"])
            .select(
                df_customer["name"].alias("customer_name"),
                "country",
                df_order["id_customer"].alias("customer_id"),
                df_order_item["id_order"].alias("order_id"),
                df_order["total"].alias("order_total"),
                df_order["date"].cast("date").alias("order_date"),
                df_order["product_id"],
                df_product["name"].alias("product_name"),
                df_product["category_id"].alias("product_category_id"),
                df_product["category_name"],
                df_order_item["seq"].alias("item_seq"),
                df_order_item["desc_item"].alias("item_description"),
                df_order_item["qty"].alias("item_quantity"),
                df_order_item["total_item"].alias("item_total"),
                (df_order_item["qty"] * df_product["unit_price"]).cast("double").alias("line_revenue"),
                df_product["unit_price"].alias("unit_price_at_sale"),
            )
        )

    def run(self):
        self.logger.info("generating orders")
        seed_date = self.config.get_value("seed_date")

        df_customer = self.spark.read.table("raw.customer")
        df_product = self.spark.read.table("raw.product")

        first_run = (
            not self.spark.catalog.tableExists("curated.order_enriched")
            or self.spark.table("curated.order_enriched").isEmpty()
        )

        if first_run:
            # Backfill: the initial 2M orders span ~1 year of dates, so we can't filter
            # by seed_date — process the whole table once. Every row freezes at the
            # current price (synthetic backfill has no historical price to honour).
            df_order = self.spark.read.table("raw.order")
            df_order_item = self.spark.read.table("raw.order_item")
            df_out = self.enrich_order(df_customer, df_order, df_order_item, df_product)
            (df_out.write.mode("overwrite").option("overwriteSchema", "false").saveAsTable("curated.order_enriched"))
        else:
            # Daily incremental: only orders for seed_date are new (raw.order.date is the
            # source string date). Enrich just those and INSERT (no UPDATE) — existing
            # rows keep their own frozen price, so a later price change never restates
            # already-booked revenue.
            df_order = self.spark.read.table("raw.order").filter(F.col("date") == seed_date)
            df_order_item = self.spark.read.table("raw.order_item")
            df_new = self.enrich_order(df_customer, df_order, df_order_item, df_product)
            df_new.createOrReplaceTempView("_silver_new")
            self.spark.sql("""
                MERGE INTO curated.order_enriched AS t
                USING _silver_new AS s ON t.order_id = s.order_id AND t.item_seq = s.item_seq
                WHEN NOT MATCHED THEN INSERT *
            """)

        self.cluster_by("curated.order_enriched", "order_date")
