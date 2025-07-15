import pyspark.sql.functions as F
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession


class Config:
    params = dict()

    def __init__(self, args):
        print("args: " + str(args))

        self.params.update({"task": args.task})
        self.params.update({"skip": args.skip})
        self.params.update({"debug": args.debug})
        self.params.update({"schema": args.schema})
        self.params.update({"env": args.env})

        self.spark = SparkSession.builder.appName(args.task).getOrCreate()

        try:
            from pyspark.dbutils import DBUtils

            self.dbutils = DBUtils(self.spark)

            # TODO cannot access context on serverless
            # context_tags = self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()
            # print(context_tags)

            # username = context_tags.get("user")

            # if username.isDefined():
            #     actual_value = username.get()
            #     python_string = str(actual_value)
            #     self.params.update({"workspace_user": python_string})
            #     print("workspace user: " + python_string)
            # else:
            #     print("workspace user empty")

        except ModuleNotFoundError:
            self.dbutils = self._mock_dbutils(self.spark)

        if self.params["env"] != "local":
            # if running in Databricks, set default catalog and schema

            if args.env == "dev":
                catalog = args.user
            else:
                catalog = args.env

            self.params.update({"catalog": catalog})

            print("Setting default catalog: " + catalog)

            self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
            self.spark.sql(f"USE CATALOG {catalog}")

            print("Setting default schema: " + args.schema)

            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.schema}")

            ws = WorkspaceClient()

        else:
            from unittest.mock import MagicMock

            ws = MagicMock(spec=WorkspaceClient, **{"current_user.me.return_value": None})

        self.dq_engine = DQEngine(ws)

    def _mock_dbutils(self, spark):
        class DBUtils:
            def __init__(self, spark):
                self.fs = self.FileSystem()

            class FileSystem:
                def mount(self, source, mount_point):
                    print(f"Mounting {source} to {mount_point}")

                def unmount(self, mount_point):
                    print(f"Unmounting {mount_point}")

                def mounts(self):
                    return []

        return DBUtils(spark)

    def get_spark(self):
        return self.spark

    def get_dbutils(self):
        return self.dbutils

    def get_value(self, key):
        return self.params[key]

    def skip_task(self):
        if self.params["skip"]:
            print("Skipped with task arg.")
            return True
        elif self.params["env"] in ("dev", "staging", "prod") and self.in_table_for_skip(self.params["task"]):
            print("Skipped with config table for 'prod' env.")
            return True

        return False

    def get_test_output(self):
        return self.params

    def in_table_for_skip(self, task):
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS system")
        schema = "task STRING, description STRING"
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS system.config ({schema})")

        df = self.spark.read.table("system.config").filter(F.col("task") == task)

        if df.count() > 0:
            return True
        else:
            return False
