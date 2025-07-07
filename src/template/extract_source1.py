from .baseTask import BaseTask


class ExtractSource1(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def run(self):
        print("Extracting data from Source1 ...")

        df = self.spark.read.table("external_source.customer")

        if self.config.get_value("debug"):
            df.show()

        df.write.mode("overwrite").saveAsTable(f"{self.config.get_value('schema')}.customer")
