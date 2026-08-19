"""
Pipeline entry point: silver validation.

Runs referential integrity checks between silver tables before gold build
scripts are allowed to run.
"""

from pyspark.sql import SparkSession

import inspect
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(inspect.currentframe().f_code.co_filename), "..", "..")))

from src.silver.referential_integrity import assert_no_orphans

def main():
    spark = SparkSession.builder.getOrCreate()

    orders = spark.table("shopease.silver.orders")
    customers = spark.table("shopease.silver.customers")

    assert_no_orphans(
        child_df=orders,
        parent_df=customers,
        child_key="customer_id",
        parent_key="customer_id",
        label="orders -> customers",
    )
    print("[silver_validation] no orphaned orders found")

if __name__ == "__main__":
    main()
