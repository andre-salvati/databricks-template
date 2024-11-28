import configparser
import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

import template


class Config:
    params = dict()

    def __init__(self, args):
        print("args: " + str(args))

        self.params.update({"task": args.task})
        self.params.update({"skip": args.skip})
        self.params.update({"debug": args.debug})
        self.params.update({"env": args.env})
        self.params.update({"default_schema": args.default_schema})

        self.spark = SparkSession.builder.appName(args.task).getOrCreate()

        try:
            from pyspark.dbutils import DBUtils

            self.dbutils = DBUtils(self.spark)

            context_tags = self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()
            username = context_tags.get("user")

            if username.isDefined():
                actual_value = username.get()
                python_string = str(actual_value)
                self.params.update({"workspace_user": python_string})
                print("workspace user: " + python_string)
            else:
                print("workspace user empty")

        except ModuleNotFoundError:
            self.dbutils = self._mock_dbutils(self.spark)

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
        elif self.params["env"] in ("dev") and not self.in_config_for_run(
            self.params["workspace_user"], self.params["task"]
        ):
            print("Skipped with config file for 'dev' and 'ci' envs.")
            return True
        elif self.params["env"] in ("stag", "prod") and self.in_table_for_skip(self.params["task"]):
            print("Skipped with config table for 'prod' env.")
            return True

        return False

    def get_test_output(self):
        return self.params

    def in_config_for_run(self, dev, task):
        config = configparser.ConfigParser()
        config_path = os.path.join(os.path.dirname(template.__file__), "config.ini")
        config.read(config_path)

        try:
            value = config.getboolean(dev, task)
        except configparser.NoOptionError:
            value = False

        return value

    def in_table_for_skip(self, task):
        self.spark.sql(f"USE CATALOG {self.params['env']}")
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS system")
        schema = "task STRING, description STRING"
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS system.config ({schema})")

        df = self.spark.read.table("system.config").filter(F.col("task") == task)

        if df.count() > 0:
            return True
        else:
            return False
