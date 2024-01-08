from funcy import print_durations
from pyspark.sql import functions as F
from pyspark.sql.types import *

from tasks.baseTask import *
from tasks import commonSchemas as sc

class Task2(BaseTask):

    def __init__(self, spark, config):
        
        super().__init__(spark, config)

    @print_durations
    def transf2(self, df):

        #TODO Replace createDataFrame() by your transformation
        data = [("task2", "transf2")]
        return self.spark.createDataFrame(data, schema=sc.schema_template)

    @print_durations
    def transf3(self, df):

        #TODO Replace createDataFrame() by your transformation
        data = [("task2", "transf3")]
        return self.spark.createDataFrame(data, schema=sc.schema_template)
    
    def run(self):

        #TODO Replace createDataFrame() by something like df = self.spark.read....
        data = [("task1", "")]
        df = self.spark.createDataFrame(data, schema=sc.schema_template)

        df_transf2 = self.transf2(df)

        self.transf3(df_transf2)

        #TODO df_out.write...