"""
Pipeline entry point: silver incremental processing.

Reads the latest bronze rows and merges them into the silver tables using
the reusable incremental-merge helper.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import inspect
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(inspect.currentframe().f_code.co_filename), "..", "..")))

from src.silver.silver_transformation_framework import merge_incremental, dedupe_latest, enforce_not_null

def main():
    spark = SparkSession.builder.getOrCreate()

    orders = spark.table("shopease.bronze.orders")
    orders = enforce_not_null(orders, ["order_id", "customer_id"])
    orders = dedupe_latest(orders, ["order_id"], "_ingested_at")
    orders = orders.withColumn("order_date", F.to_date("order_timestamp"))

    merge_incremental(
        spark,
        source_df=orders,
        target_table="shopease.silver.orders",
        merge_condition="target.order_id = source.order_id",
    )
    print("[silver_incremental_processing] merged orders into shopease.silver.orders")

if __name__ == "__main__":
    main()
