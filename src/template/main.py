import argparse

from .config import Config
from .job1.extract_source1 import ExtractSource1
from .job1.extract_source2 import ExtractSource2
from .job1.generate_orders import GenerateOrders
from .job1.generate_orders_agg import GenerateOrdersAgg
from .job1.integration_setup import Setup
from .job1.integration_validate import Validate


def arg_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument("--user")
    parser.add_argument("--env", required=True, choices=["dev", "staging", "prod"])
    parser.add_argument(
        "--task",
        required=True,
        choices=["extract_source1", "extract_source2", "generate_orders", "generate_orders_agg", "setup", "validate"],
    )
    parser.add_argument("--schema")
    parser.add_argument("--skip", action="store_true")
    parser.add_argument("--debug", action="store_true")

    return parser


def main():
    args = arg_parser().parse_args()

    config = Config(args)

    if not config.skip_task():
        if args.task == "extract_source1":
            ExtractSource1(config).run()
        elif args.task == "extract_source2":
            ExtractSource2(config).run()
        elif args.task == "generate_orders":
            GenerateOrders(config).run()
        elif args.task == "generate_orders_agg":
            GenerateOrdersAgg(config).run()
        elif args.task == "setup":
            Setup(config).run()
        elif args.task == "validate":
            Validate(config).run()


if __name__ == "__main__":
    main()
