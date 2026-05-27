import argparse
import logging
import sys

from tests.job1.integration_setup import Setup
from tests.job1.integration_validate import Validate

from .config import Config
from .job1.extract_source1 import ExtractSource1
from .job1.extract_source2 import ExtractSource2
from .job1.generate_orders import GenerateOrders
from .job1.generate_orders_agg import GenerateOrdersAgg
from .job1.health_check import HealthCheck
from .job1.seed_sources import SeedSources

TASKS = {
    "extract_source1": ExtractSource1,
    "extract_source2": ExtractSource2,
    "generate_orders": GenerateOrders,
    "generate_orders_agg": GenerateOrdersAgg,
    "health_check": HealthCheck,
    "seed_sources": SeedSources,
    "setup": Setup,
    "validate": Validate,
}


def arg_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument("--env", required=True, choices=["dev", "staging", "prod"])
    parser.add_argument("--task", required=True, choices=sorted(TASKS.keys()))
    # Pure observability — filled by Databricks at runtime via {{job.run_id}};
    # there's no equivalent env var on serverless compute.
    parser.add_argument("--run-id")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARN", "WARNING"])
    parser.add_argument("--quarantine-fail-ratio", type=float, default=1.0)
    # ISO-8601 date (YYYY-MM-DD). Empty string or absent → resolved to today by Config.
    # Filled by {{job.parameters.seed_date}}; override per-run for backfills.
    parser.add_argument("--seed-date", default=None)

    return parser


def main():
    args = arg_parser().parse_args()

    config = Config(args)

    try:
        TASKS[args.task](config).run()
    except Exception:
        # Ensure tracebacks land in the Databricks driver log even when stdout is buffered.
        logging.getLogger("template").exception("task %s failed", args.task)
        sys.exit(1)


if __name__ == "__main__":
    main()
