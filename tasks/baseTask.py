import inspect

from funcy import print_durations
from pyspark.sql import Window
from pyspark.sql import functions as F

class BaseTask:    

    def __init__(self, spark, config):

        self.spark = spark
        self.config = config

   