import logging


class BaseTask:
    def __init__(self, config):
        self.config = config
        self.spark = config.get_spark()
        self.logger = logging.getLogger(f"template.{self.__class__.__name__}")

    def cluster_by(self, table: str, *cols: str) -> None:
        """Set Delta liquid-clustering keys on an existing table.

        Metadata-only and idempotent: re-running just re-asserts the keys.
        Subsequent writes (append / MERGE / replaceWhere) and OPTIMIZE cluster
        the data by these columns. Only pays off on accumulating tables — we do
        not cluster full-overwrite tables (raw.*), where clustering can't amortize.
        """
        self.spark.sql(f"ALTER TABLE {table} CLUSTER BY ({', '.join(cols)})")
