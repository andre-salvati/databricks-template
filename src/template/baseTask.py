import logging


class BaseTask:
    def __init__(self, config):
        self.config = config
        self.spark = config.get_spark()
        self.logger = logging.getLogger(f"template.{self.__class__.__name__}")
