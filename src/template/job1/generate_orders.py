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
                "name",
                "country",
                "id_customer",
                "id_order",
                "total",
                "date",
                "product_id",
                "prod_category_id",
                "seq",
                "desc_item",
                "qty",
                "total_item",
            )
        )

    def run(self):
        self.logger.info("generating orders")

        df_customer = self.spark.read.table("raw.customer")
        df_order = self.spark.read.table("raw.order")
        df_order_item = self.spark.read.table("raw.order_item")

        df_out = self.enrich_order(df_customer, df_order, df_order_item)

        (df_out.write.mode("overwrite").option("overwriteSchema", "false").saveAsTable("curated.order_enriched"))
