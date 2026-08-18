"""
Gold layer: business-level aggregates for the ShopEase data platform.

Reads silver Delta tables and builds analytics-ready marts such as daily
sales summaries and customer lifetime value.
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

def build_daily_sales(spark, catalog, silver_schema):
    orders = spark.table(f"{catalog}.{silver_schema}.orders")
    return (
        orders.groupBy("order_date")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("order_total").alias("gross_revenue"),
            F.countDistinct("customer_id").alias("distinct_customers"),
        )
        .orderBy("order_date")
    )

def build_customer_ltv(spark, catalog, silver_schema):
    orders = spark.table(f"{catalog}.{silver_schema}.orders")
    customers = spark.table(f"{catalog}.{silver_schema}.customers")

    order_agg = orders.groupBy("customer_id").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("order_total").alias("lifetime_value"),
        F.max("order_date").alias("last_order_date"),
    )

    return customers.join(order_agg, on="customer_id", how="left").fillna(
        {"total_orders": 0, "lifetime_value": 0.0}
    )

def main():
    spark = SparkSession.builder.getOrCreate()

    catalog = get_param("catalog", "shopease")
    silver_schema = get_param("silver_schema", "silver")
    gold_schema = get_param("gold_schema", "gold")

    marts = {
        "daily_sales": build_daily_sales,
        "customer_ltv": build_customer_ltv,
    }

    for table_name, build_fn in marts.items():
        df = build_fn(spark, catalog, silver_schema)
        target = f"{catalog}.{gold_schema}.{table_name}"
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
        print(f"Wrote {df.count()} rows to {target}")

if __name__ == "__main__":
    main()
