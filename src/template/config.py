import logging
import re

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s [run=%(run_id)s] :: %(message)s"

# Renamed from "system" to avoid colliding with Unity Catalog's reserved `system` catalog.
INTERNAL_SCHEMA = "ops"

# Single source of truth for the schemas this template expects in every catalog.
# Dev catalogs create these on demand (Config.__init__).
# Staging/prod schemas are pre-provisioned by make init (sdk_init_workspace.py)
# and are never created at runtime — the job's run-as identity has no CREATE SCHEMA privilege.
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
        self.params: dict = {}

        self.params.update({"task": args.task})
        self.params.update({"env": args.env})

        # --log-level is a job-level parameter set in sdk_generate_template_job.py.
        # Operators can override it per-run via the Jobs UI "Run with different parameters".
        log_level = args.log_level.upper()
        self.params.update({"log_level": log_level})
        self.params.update({"quarantine_fail_ratio": args.quarantine_fail_ratio})

        # run_id comes from --run-id={{job.run_id}}. Not exposed as an env var on serverless,
        # so it has to be threaded through as a CLI param. Falls back to "-" for local tests.
        run_id = getattr(args, "run_id", None) or "-"
        _configure_logging(getattr(logging, log_level, logging.INFO), run_id)
        self.logger = logging.getLogger("template")
        self.logger.info(
            "config init task=%s env=%s log_level=%s quarantine_fail_ratio=%s",
            args.task,
            args.env,
            log_level,
            args.quarantine_fail_ratio,
        )

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
            if args.env == "dev":
                # Dev sandbox: create schemas on first use so a developer can
                # run any task without manual bootstrapping.
                # staging/prod schemas are pre-provisioned by make init and must
                # not be recreated at runtime (no CREATE SCHEMA privilege).
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

    def get_test_output(self):
        return self.params
