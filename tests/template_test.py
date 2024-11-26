from argparse import Namespace

import pytest
from pyspark.sql import *
from pyspark.sql.types import *

from template.main import *
from template.config import Config as TaskConfig
from template.extract_source1 import ExtractSource1
from template.extract_source2 import ExtractSource2

from pyspark.testing import assertDataFrameEqual

from template.commonSchemas import schema_template

#from databricks.connect import DatabricksSession
#from databricks.sdk.core import Config


@pytest.fixture
def spark() -> DataFrame:

   #config = Config(profile = "DEV")
   #return DatabricksSession.builder.sdkConfig(config).getOrCreate()
   return SparkSession.builder.appName('unit-tests').getOrCreate()

@pytest.fixture
def config() -> TaskConfig:

   return TaskConfig(Namespace(task='extract_source1', env='local', default_catalog='dev', default_schema='template', skip=False, debug=True))

@pytest.fixture
def spark(config) -> TaskConfig:
    return config.get_spark()

def test_arg_parser():

   parser = arg_parser()

   args = parser.parse_args(["--task=extract_source1", "--env=dev", "--default_schema=template", "--skip", "--debug"])

   assert args == Namespace(task='extract_source1', env='dev', default_schema='template', skip=True, debug=True)

@pytest.mark.parametrize("args, expected_output", [
   (Namespace(task='extract_source1', env='dev', skip=False, debug=True, default_schema='dev', default_catalog='template'),
      {'task':'extract_source1', 'env':'dev', 'skip':False, 'debug':True, 'default_schema':'dev'}),
])
def test_config(args, expected_output):

   config = TaskConfig(args)

   assert config.get_test_output() == expected_output

def test_transf1(spark, config):

   task = ExtractSource1(config)

   input_df = spark.createDataFrame([], schema=schema_template)

   df_out = task.transf1(input_df)

   assert df_out.count() == 1

   expected_df = spark.createDataFrame( [("task1", "transf1")], schema=schema_template)

   assertDataFrameEqual(df_out, expected_df)
