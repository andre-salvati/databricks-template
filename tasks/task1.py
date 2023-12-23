from funcy import print_durations
from pyspark.sql import functions as F
from pyspark.sql.types import *

from tasks.baseTask import *
from tasks import commonSchemas as sc

class Task1(BaseTask):

    def __init__(self, spark, config):

        super().__init__(spark, config)

    @print_durations
    def transf1(self):

        data = [("task1", "transf1")]

        return self.spark.createDataFrame(data, schema=sc.schema_template)

    def run(self):

        self.transf1()