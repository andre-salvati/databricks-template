import argparse

from pyspark import *
from pyspark.sql import *

from tasks.config import *
from tasks.task1 import *
from tasks.task2 import *

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
        
    parser.add_argument("--env", choices=["dev", "prod"])
    parser.add_argument("--input")
    parser.add_argument("--output")  
    parser.add_argument("--task", required=True, choices=["set_parameters",
                                                            "task1",
                                                            "task2"])
    parser.add_argument("--skip", action='store_true')
    parser.add_argument("--only_delta", action='store_true')

    args = parser.parse_args()

    print("args: " + str(args))

    if args.task == "set_parameters":

        if args.env is None:
            parser.error("--env required")

        if args.input is None:
            parser.error("--input required")

        print("Setting workflow configs... ")
        dbutils.jobs.taskValues.set(key = 'env', value = args.env)
        dbutils.jobs.taskValues.set(key = 'input', value = args.input)

        if args.output:
            dbutils.jobs.taskValues.set(key = 'output', value = args.output)
 
    elif not args.skip:

        try:
            env = dbutils.jobs.taskValues.get(taskKey = "set_parameters", key = 'env')
        except ValueError as ve:
            if args.env is None:
                parser.error("--env required")
            else:
                env = args.env

        try:
            input = dbutils.jobs.taskValues.get(taskKey = "set_parameters", key = 'input')
        except ValueError as ve:
            if args.input is None:
                parser.error("--input required")
            else:
                input = args.input

        try:
            output = dbutils.jobs.taskValues.get(taskKey = "set_parameters", key = 'output')
        except ValueError as ve:
            output = args.output
            
        config = Config(env, input, output, args.task, args.only_delta)

        spark = SparkSession.builder.appName(args.task).getOrCreate()

        if args.task == "task1":
            Task1(spark, config).run()
        elif args.task == "task2":
            Task2(spark, config).run()
    
    else:
        
        print(args.task + " skipped")
  