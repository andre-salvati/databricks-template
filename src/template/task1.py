from funcy import print_durations
from pyspark.sql import functions as F
from pyspark.sql.types import *

from .baseTask import *
from .commonSchemas import *

class Task1(BaseTask):

    def __init__(self, spark, config):

        super().__init__(spark, config)

    @print_durations
    def transf1(self, df):

        #TODO code your transformations here... 

        data = [("task1", "transf1")]
        df_new = self.spark.createDataFrame(data, schema=schema_template)
        
        return df.union(df_new)

    def run(self):

        # prepare a simple input table

        self.spark.sql("CREATE CATALOG IF NOT EXISTS template")

        self.spark.sql("USE CATALOG template")

        self.spark.sql("CREATE SCHEMA IF NOT EXISTS test")

        self.spark.sql("USE SCHEMA test")

        df = self.spark.createDataFrame([], schema=schema_template)

        df.write.mode("overwrite").saveAsTable("table1")

        # run transformations

        df_in = self.spark.read.table("table1")

        df_out = self.transf1(df_in)

        if self.config.get_value("debug"):
            df_out.show()
            
        df_out.write.mode("overwrite").saveAsTable("table2")
