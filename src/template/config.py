import logging
import os
import re

import pyspark.sql.functions as F
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s [run=%(run_id)s] :: %(message)s"

# Schema used internally by the template for runtime config (e.g. tasks to skip).
# Renamed from "system" to avoid colliding with Unity Catalog's reserved `system` catalog.
INTERNAL_SCHEMA = "ops"

# Single source of truth for the schemas this template expects in every catalog.
# Created on Config init (idempotent) so any task can read/write any layer regardless
# of which task happens to run first.
MEDALLION_SCHEMAS = ["external_source", "raw", "curated", "report", INTERNAL_SCHEMA]


class _RunIdFilter(logging.Filter):
    # Stamps every log record with the Databricks job run_id so logs can be
    # correlated to a specific run after they're ingested into Splunk/Datadog/etc.
    def __init__(self, run_id: str):
        super().__init__()
        self.run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = self.run_id
        return True


def _configure_logging(level: int, run_id: str) -> None:
    # Attach the handler to our "template" logger only — not the root logger —
    # so framework loggers (py4j, etc.) don't try to emit through our handler
    # during interpreter teardown when stdout has already been closed.
    logger = logging.getLogger("template")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(_LOG_FORMAT))
        handler.addFilter(_RunIdFilter(run_id))
        logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False


class Config:
    def __init__(self, args):
        # Instance-scoped — earlier versions used a class-level dict which leaked across instances.
        self.params: dict = {}

        self.params.update({"task": args.task})
        self.params.update({"skip": args.skip})
        self.params.update({"env": args.env})

        # Override at runtime via TEMPLATE_LOG_LEVEL env var (e.g. set DEBUG from the
        # Jobs UI "Run with different parameters" dialog when investigating prod incidents).
        log_level = os.environ.get("TEMPLATE_LOG_LEVEL", "INFO").upper()
        # run_id comes from --run-id={{job.run_id}}. Not exposed as an env var on serverless,
        # so it has to be threaded through as a CLI param. Falls back to "-" for local tests.
        run_id = getattr(args, "run_id", None) or "-"
        _configure_logging(getattr(logging, log_level, logging.INFO), run_id)
        self.logger = logging.getLogger("template")
        self.logger.info("config init task=%s env=%s", args.task, args.env)

        self.spark = SparkSession.builder.appName(args.task).getOrCreate()

        if args.env != "local":
            # if running in Databricks, set default catalog and schema
            ws = WorkspaceClient()

            if args.env == "dev":
                # Per-developer sandbox catalog; created on first run by the developer
                # themselves (not the SP). The `dev_` prefix isolates these from the
                # shared `staging` / `prod` catalogs in the workspace catalog listing.
                # Identity comes from the executing run_as user — in dev that's the developer.
                # Sanitize the email local-part into a valid SQL identifier
                # (e.g. "andre.f.salvati@gmail.com" -> "andre_f_salvati").
                local_part = ws.current_user.me().user_name.split("@")[0]
                user = re.sub(r"[^a-zA-Z0-9_]", "_", local_part)
                catalog = f"dev_{user}"
                self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
            else:
                # staging/prod catalogs are provisioned by scripts/sdk_init_workspace.py
                # at deployment time; runtime jobs must not have CREATE CATALOG privilege.
                catalog = args.env

            self.params.update({"catalog": catalog})

            self.logger.info("using catalog=%s", catalog)

            self.spark.sql(f"USE CATALOG {catalog}")
            for s in MEDALLION_SCHEMAS:
                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s}")

        else:
            from unittest.mock import MagicMock

            ws = MagicMock(spec=WorkspaceClient, **{"current_user.me.return_value": None})

        self.dq_engine = DQEngine(ws)

    def get_spark(self):
        return self.spark

    def get_value(self, key):
        return self.params[key]

    def skip_task(self):
        if self.params["skip"]:
            self.logger.info("skipping task: --skip flag set")
            return True
        elif self.params["env"] in ("dev", "staging", "prod") and self._in_skip_table(self.params["task"]):
            self.logger.info("skipping task: present in %s.config skip table", INTERNAL_SCHEMA)
            return True

        return False

    def get_test_output(self):
        return self.params

    def _in_skip_table(self, task):
        # Schema `ops` is created in __init__ via MEDALLION_SCHEMAS.
        schema_ddl = "task STRING, description STRING"
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {INTERNAL_SCHEMA}.config ({schema_ddl})")

        df = self.spark.read.table(f"{INTERNAL_SCHEMA}.config").filter(F.col("task") == task)

        return df.count() > 0
