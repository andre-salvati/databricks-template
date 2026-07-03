from ..baseTask import BaseTask

# Bronze dimension copies. Both are full overwrites of small dimension tables, so
# they are intentionally not liquid-clustered (clustering can't amortise under a
# daily full rewrite). The mutable attribute (product.name) is carried faithfully
# so the silver layer sees the current product name on each run.
DIMENSIONS = [
    ("external_source.customer", "raw.customer"),
    ("external_source.product", "raw.product"),
]


class ExtractSource1(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def run(self):
        self.logger.info("extracting data from Source1")

        for input_table, output_table in DIMENSIONS:
            df = self.spark.read.table(input_table)
            # overwriteSchema=false: fail loudly on upstream schema drift instead of silently propagating it.
            (df.write.mode("overwrite").option("overwriteSchema", "false").saveAsTable(output_table))
