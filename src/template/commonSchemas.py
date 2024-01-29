from pyspark.sql.types import *

schema_template = StructType([
    StructField("task", StringType(), True),
    StructField("transformation", StringType(), True)
])