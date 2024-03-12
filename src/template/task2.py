from funcy import print_durations
from pyspark.sql import functions as F
from pyspark.sql.types import *

from .baseTask import *
from .commonSchemas import *

class Task2(BaseTask):

    def __init__(self, spark, config):
        
        super().__init__(spark, config)

    @print_durations
    def transf2(self, df):

        #TODO code your transformations here... 

        data = [("task2", "transf2")]
        df_new = self.spark.createDataFrame(data, schema=schema_template)
        
        return df.union(df_new)
    
    @print_durations
    def transf3(self, df):

        #TODO code your transformations here... 

        data = [("task2", "transf3")]
        df_new = self.spark.createDataFrame(data, schema=schema_template)
        
        return df.union(df_new)    
    
    def run(self):

        self.spark.sql("USE CATALOG template")

        self.spark.sql("USE SCHEMA test")
      
        df_in = self.spark.read.table("table2")

        df_transf2 = self.transf2(df_in)

        df_out = self.transf3(df_transf2)

        if self.config.get_value("debug"):
            df_out.show()
            
        df_out.write.mode("overwrite").saveAsTable("table3")
