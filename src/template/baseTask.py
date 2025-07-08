class BaseTask:
    def __init__(self, config):
        self.config = config
        self.spark = config.get_spark()
        self.dbutils = config.get_dbutils()
