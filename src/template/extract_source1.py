from funcy import print_durations

from .baseTask import BaseTask
from .commonSchemas import schema_template

schema = "raw_source1"


class ExtractSource1(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    @print_durations
    def transf1(self, df):
        # TODO code your transformations here...

        data = [("task1", "transf1")]
        df_new = self.spark.createDataFrame(data, schema=schema_template)

        return df.union(df_new)

    def run(self):
        print("Extracting data from Source1 ...")

        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        # prepare a simple input table

        df = self.spark.createDataFrame([], schema=schema_template)

        df.write.mode("overwrite").saveAsTable(f"{schema}.table1")

        # run transformations

        df_in = self.spark.read.table(f"{schema}.table1")

        df_out = self.transf1(df_in)

        if self.config.get_value("debug"):
            df_out.show()

        df_out.write.mode("overwrite").saveAsTable(f"{schema}.table2")
