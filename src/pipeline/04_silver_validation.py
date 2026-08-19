"""
Pipeline entry point: silver validation.

Runs referential integrity checks between silver tables before gold build
scripts are allowed to run.
"""

from pyspark.sql import SparkSession

import os
import sys
sys.path.append(os.path.abspath("../.."))

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
"""
Pipeline entry point: silver validation.

Runs referential integrity checks between silver tables before gold build
scripts are allowed to run.
"""

from pyspark.sql import SparkSession

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
