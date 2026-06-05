from pyspark.sql.types import (
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
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
        StructField("product_id", IntegerType(), True),
        StructField("prod_category_id", IntegerType(), True),
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

order_enriched_schema = StructType(
    [
        StructField("customer_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("order_total", FloatType(), True),
        StructField("order_date", DateType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("product_category_id", IntegerType(), True),
        StructField("item_seq", IntegerType(), True),
        StructField("item_description", StringType(), True),
        StructField("item_quantity", IntegerType(), True),
        StructField("item_total", FloatType(), True),
    ]
)

order_agg_schema = StructType(
    [
        StructField("customer_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("product_category_id", IntegerType(), True),
        StructField("total_quantity", LongType(), True),
        StructField("total_value", DoubleType(), True),
        StructField("total_orders", LongType(), True),
    ]
)
