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

        #TODO Replace createDataFrame() by your transformation
        data = [("task1", "transf1")]
        return self.spark.createDataFrame(data, schema=schema_template)

    def run(self):

        #TODO Replace createDataFrame() by something like df = self.spark.read....
        data = [("task1", "")]
        df = self.spark.createDataFrame(data, schema=schema_template)

        df_out = self.transf1(df)

        if self.config.get_value("debug"):
            print("run code for debug mode")

        #TODO df_out.write...
