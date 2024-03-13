import inspect

from funcy import print_durations
from pyspark.sql import Window
from pyspark.sql import functions as F

class BaseTask:    

    def __init__(self, spark, config):

        self.spark = spark
        self.config = config

        env = self.config.get_value("env")

        if not env == "local":

            catalog = self.config.get_value("default_catalog")
            schema = self.config.get_value("default_schema")

            print("Creating catalog '" + catalog + "' ..." )

            self.spark.sql("CREATE CATALOG IF NOT EXISTS " + catalog)

            self.spark.sql("USE CATALOG " + catalog)

            print("Creating schema '" + schema + "' ..." )
            
            self.spark.sql("CREATE SCHEMA IF NOT EXISTS " + schema)

            self.spark.sql("USE SCHEMA " + schema)