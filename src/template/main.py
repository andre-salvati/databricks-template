import argparse

from .config import Config
from .extract_source1 import ExtractSource1
from .extract_source2 import ExtractSource2


def arg_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument("--env", required=True, choices=["dev", "ci", "prod"])
    parser.add_argument("--default_schema")
    parser.add_argument("--task", required=True, choices=["extract_source1", "extract_source2"])
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


if __name__ == "__main__":
    main()
