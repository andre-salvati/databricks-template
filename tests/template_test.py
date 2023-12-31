import datetime

import pytest
from chispa.dataframe_comparer import *
from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.types import *

from tasks import commonSchemas as sc
from tasks.config import Config as TaskConfig
from tasks.task1 import *
from tasks.task2 import *

#from databricks.connect import DatabricksSession
#from databricks.sdk.core import Config

@pytest.fixture
def spark() -> DataFrame:

   #config = Config(profile = "DEV")
   #return DatabricksSession.builder.sdkConfig(config).getOrCreate()
   return SparkSession.builder.appName('unit-tests').getOrCreate()

@pytest.fixture
def df_in(spark) -> DataFrame:
   
   data = [("task1", "")]
   return spark.createDataFrame(data, schema=sc.schema_template)

@pytest.fixture
def config() -> TaskConfig:

   return TaskConfig("", "", "", "", False)

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

