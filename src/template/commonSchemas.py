from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

customer_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("country", StringType(), True),
    ]
)

order_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("id_customer", IntegerType(), True),
        StructField("total", FloatType(), True),
        StructField("date", StringType(), True),
    ]
)

order_item_schema = StructType(
    [
        StructField("id_order", IntegerType(), True),
        StructField("seq", IntegerType(), True),
        StructField("desc_item", StringType(), True),
        StructField("qty", IntegerType(), True),
        StructField("total_item", FloatType(), True),
    ]
)
