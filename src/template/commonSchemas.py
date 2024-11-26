from pyspark.sql.types import StringType, StructField, StructType

schema_template = StructType(
    [StructField("task", StringType(), True), StructField("transformation", StringType(), True)]
)
