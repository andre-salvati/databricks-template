from funcy import print_durations
from pyspark.sql import functions as F
from pyspark.sql.types import *

from tasks.baseTask import *
from tasks import commonSchemas as sc

class Task2(BaseTask):

    def __init__(self, spark, config):
        
        super().__init__(spark, config)

    @print_durations
    def transf2(self):

        data = [("task2", "transf2")]

        return self.spark.createDataFrame(data, schema=sc.schema_template)

    @print_durations
    def transf3(self):

        data = [("task2", "transf3")]

        return self.spark.createDataFrame(data, schema=sc.schema_template)
    
    def run(self):

        #TODO read

        self.transf2()

        self.transf3()

        #TODO write