class BaseTask:
    def __init__(self, config):
        self.config = config
        self.spark = config.get_spark()
        self.dbutils = config.get_dbutils()

        if config.get_value("env") != "local":
            catalog = config.get_value("default_catalog")

            print("Setting default catalog " + catalog)

            self.spark.sql("USE CATALOG " + catalog)
