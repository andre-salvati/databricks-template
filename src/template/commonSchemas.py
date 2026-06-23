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

# Product dimension. `name` is mutable: the daily seed renames a couple of products
# over time (e.g. "Product 1" → "Product 1.1"), which is what makes the silver
# "freeze at sale time" vs "restate" distinction observable — silver freezes the
# product_name onto each order line at processing time. unit_price is a static
# attribute (set once, never bumped). category_id/category_name are stable too.
product_schema = StructType(
    [
        StructField("product_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("unit_price", FloatType(), True),
        StructField("category_id", IntegerType(), True),
        StructField("category_name", StringType(), True),
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
        StructField("product_name", StringType(), True),
        StructField("product_category_id", IntegerType(), True),
        StructField("category_name", StringType(), True),
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
        StructField("product_name", StringType(), True),
        StructField("product_category_id", IntegerType(), True),
        StructField("category_name", StringType(), True),
        StructField("total_quantity", LongType(), True),
        # total_value is SUM(item_total) — the line value frozen on the order at sale time.
        StructField("total_value", DoubleType(), True),
        StructField("total_orders", LongType(), True),
    ]
)
