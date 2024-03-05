from argparse import Namespace

import pytest
from chispa.dataframe_comparer import *
from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.types import *

from template.main import *
from template import commonSchemas as sc
from template.config import Config as TaskConfig
from template.task1 import *
from template.task2 import *
from template.task1 import *

#from databricks.connect import DatabricksSession
#from databricks.sdk.core import Config

@pytest.fixture
def spark() -> DataFrame:

   #config = Config(profile = "DEV")
   #return DatabricksSession.builder.sdkConfig(config).getOrCreate()
   return SparkSession.builder.appName('unit-tests').getOrCreate()

@pytest.fixture
def df_in(spark) -> DataFrame:
   
      return spark.createDataFrame([], schema=sc.schema_template)

@pytest.fixture
def config() -> TaskConfig:

   return TaskConfig(Namespace(task='task1', env='dev', input='2024-01-01', output=None, skip=False, debug=True))

def test_arg_parser():

   parser = arg_parser()

   args = parser.parse_args(["--task=task1", "--env=dev","--input=test", "--skip", "--debug"])

   assert args == Namespace(task='task1', env='dev', input='test', output=None, skip=True, debug=True)

@pytest.mark.parametrize("args, expected_output", [
   (Namespace(task='task1', env='dev', input='2024-01-01', output=None, skip=False, debug=True),
      {'input': 's3://dev-dbtemplate123/2024-01-01/', 'output':'s3://dev-dbtemplate123/2024-01-01/', 'skip':False, 'debug':True}),
   (Namespace(task='task2', env='prod', input='2024-01-02', output='test', skip=False, debug=False),
      {'input': 's3://prod-dbtemplate123/2024-01-02/', 'output':'s3://prod-dbtemplate123/test/', 'skip':False, 'debug':False}),
   (Namespace(task='task3', env='prod', input='2024-01-03', output=None, skip=True, debug=False), 
      {'input': 's3://prod-dbtemplate123/2024-01-03/', 'output':'s3://prod-dbtemplate123/2024-01-03/', 'skip':True, 'debug':False})
])
def test_config(args, expected_output):

   config = TaskConfig(args)

   assert config.get_test_output() == expected_output


def test_transf1(spark, config, df_in):

   task = Task1(spark, config)

   df_out = task.transf1(df_in)

   assert df_out.count() == 1

   expected_data = [("task1", "transf1")]

   expected_df = spark.createDataFrame(expected_data, schema=sc.schema_template)
   
   assert_df_equality(df_out, expected_df, ignore_row_order=True, ignore_nullable=True)


def test_transf2(spark, config, df_in):

   task = Task2(spark, config)

   df_out = task.transf2(df_in)

   assert df_out.count() == 1

   expected_data = [("task2", "transf2")]

   expected_df = spark.createDataFrame(expected_data, schema=sc.schema_template)
   
   assert_df_equality(df_out, expected_df, ignore_row_order=True, ignore_nullable=True)


def test_transf3(spark, config, df_in):

   task = Task2(spark, config)

   df_out = task.transf3(df_in)

   assert df_out.count() == 1

   expected_data = [("task2", "transf3")]

   expected_df = spark.createDataFrame(expected_data, schema=sc.schema_template)
   
   assert_df_equality(df_out, expected_df, ignore_row_order=True, ignore_nullable=True)

