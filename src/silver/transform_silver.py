"""
Silver layer: cleansing and conformance for the ShopEase data platform.

Reads bronze Delta tables, applies deduplication, null-handling, and schema
conformance, and writes the results as silver Delta tables.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def get_param(name, default=None):
    args = dict(arg.split("=", 1) for arg in sys.argv[1:] if "=" in arg)
    value = args.get(name, default)
    if value is None:
        raise ValueError(f"Missing required parameter: {name}")
    return value

def clean_orders(spark, catalog, bronze_schema):
    orders = spark.table(f"{catalog}.{bronze_schema}.orders")
    return (
        orders.dropDuplicates(["order_id"])
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("order_total").isNotNull() & (F.col("order_total") >= 0))
        .withColumn("order_date", F.to_date("order_timestamp"))
    )

def clean_customers(spark, catalog, bronze_schema):
    customers = spark.table(f"{catalog}.{bronze_schema}.customers")
    return (
        customers.dropDuplicates(["customer_id"])
        .filter(F.col("customer_id").isNotNull())
        .withColumn("email", F.lower(F.trim("email")))
    )

def clean_products(spark, catalog, bronze_schema):
    products = spark.table(f"{catalog}.{bronze_schema}.products")
    return products.dropDuplicates(["product_id"]).filter(F.col("product_id").isNotNull())

def clean_clickstream(spark, catalog, bronze_schema):
    events = spark.table(f"{catalog}.{bronze_schema}.clickstream")
    return events.filter(F.col("session_id").isNotNull() & F.col("event_type").isNotNull())

def main():
    spark = SparkSession.builder.getOrCreate()

    catalog = get_param("catalog", "shopease")
    bronze_schema = get_param("bronze_schema", "bronze")
    silver_schema = get_param("silver_schema", "silver")

    transforms = {
        "orders": clean_orders,
        "customers": clean_customers,
        "products": clean_products,
        "clickstream": clean_clickstream,
    }

    for table_name, transform_fn in transforms.items():
        df = transform_fn(spark, catalog, bronze_schema)
        target = f"{catalog}.{silver_schema}.{table_name}"
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
        print(f"Wrote {df.count()} rows to {target}")

if __name__ == "__main__":
    main()
