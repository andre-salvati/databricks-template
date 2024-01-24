import argparse

from pyspark import *
from pyspark.sql import *

from tasks.config import *
from tasks.task1 import *
from tasks.task2 import *

def arg_parser():

    parser = argparse.ArgumentParser()
        
    parser.add_argument("--env", choices=["dev", "prod"])
    parser.add_argument("--input")
    parser.add_argument("--output")  
    parser.add_argument("--task", required=True, choices=["task1", "task2"])
    parser.add_argument("--skip", action='store_true')

    return parser

if __name__ == "__main__":

    args = arg_parser().parse_args()

    config = Config(args)

    if not config.skip_task():

        spark = SparkSession.builder.appName(args.task).getOrCreate()

        if args.task == "task1":
            Task1(spark, config).run()
        elif args.task == "task2":
            Task2(spark, config).run()
    
    else:
        
        print(args.task + " skipped")