from ..baseTask import BaseTask

# Lightweight smoke task suitable for running before prod jobs to confirm the wheel,
# environment, catalog grants, and SQL warehouse are all reachable.


class HealthCheck(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def run(self):
        self.logger.info("running health check")

        table = "ops._health"
        df = self.spark.sql("SELECT current_timestamp() AS ts, 1 AS ok")
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)

        n = self.spark.read.table(table).count()
        if n != 1:
            raise RuntimeError(f"health check expected 1 row, got {n}")
        self.logger.info("health check ok")
