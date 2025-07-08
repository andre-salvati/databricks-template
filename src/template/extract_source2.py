from .baseTask import BaseTask


class ExtractSource2(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def run(self):
        print("Extracting data from Source2 ...")

        df_order = self.spark.read.table("external_source.order")
        df_order_item = self.spark.read.table("external_source.order_item")

        if self.config.get_value("debug"):
            df_order.show()
            df_order_item.show()

        df_order.write.mode("overwrite").saveAsTable(f"{self.config.get_value('schema')}.order")
        df_order_item.write.mode("overwrite").saveAsTable(f"{self.config.get_value('schema')}.order_item")
