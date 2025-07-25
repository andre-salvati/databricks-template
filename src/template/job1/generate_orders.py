from funcy import print_durations

from ..baseTask import BaseTask


class GenerateOrders(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    @print_durations
    def enrich_order(self, df_customer, df_order, df_order_item):
        # TODO code your transformations here...

        return (
            df_order_item.join(df_order, df_order_item["id_order"] == df_order["id"])
            .join(df_customer, df_order["id_customer"] == df_customer["id"])
            .select("name", "id_customer", "id_order", "total", "seq", "desc_item", "qty", "total_item")
        )

    def run(self):
        print("Generating Orders ...")

        df_customer = self.spark.read.table("raw.customer")
        df_order = self.spark.read.table("raw.order")
        df_order_item = self.spark.read.table("raw.order_item")

        df_out = self.enrich_order(df_customer, df_order, df_order_item)

        if self.config.get_value("debug"):
            df_out.show()

        df_out.write.mode("overwrite").saveAsTable(f"{self.config.get_value('schema')}.order_enriched")
