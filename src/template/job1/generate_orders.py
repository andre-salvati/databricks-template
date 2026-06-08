from ..baseTask import BaseTask


class GenerateOrders(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def enrich_order(self, df_customer, df_order, df_order_item):
        # TODO code your transformations here...

        return (
            df_order_item.join(df_order, df_order_item["id_order"] == df_order["id"])
            .join(df_customer, df_order["id_customer"] == df_customer["id"])
            .select(
                df_customer["name"].alias("customer_name"),
                "country",
                df_order["id_customer"].alias("customer_id"),
                df_order_item["id_order"].alias("order_id"),
                df_order["total"].alias("order_total"),
                df_order["date"].cast("date").alias("order_date"),
                "product_id",
                df_order["prod_category_id"].alias("product_category_id"),
                df_order_item["seq"].alias("item_seq"),
                df_order_item["desc_item"].alias("item_description"),
                df_order_item["qty"].alias("item_quantity"),
                df_order_item["total_item"].alias("item_total"),
            )
        )

    def run(self):
        self.logger.info("generating orders")

        df_customer = self.spark.read.table("raw.customer")
        df_order = self.spark.read.table("raw.order")
        df_order_item = self.spark.read.table("raw.order_item")

        df_out = self.enrich_order(df_customer, df_order, df_order_item)

        (df_out.write.mode("overwrite").option("overwriteSchema", "false").saveAsTable("curated.order_enriched"))
