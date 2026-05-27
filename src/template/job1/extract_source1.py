from ..baseTask import BaseTask

INPUT_TABLE = "external_source.customer"
OUTPUT_TABLE = "raw.customer"


class ExtractSource1(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def run(self):
        self.logger.info("extracting data from Source1")

        df = self.spark.read.table(INPUT_TABLE)

        # overwriteSchema=false: fail loudly on upstream schema drift instead of silently propagating it.
        (df.write.mode("overwrite").option("overwriteSchema", "false").saveAsTable(OUTPUT_TABLE))
