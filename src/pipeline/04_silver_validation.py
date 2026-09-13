"""
Pipeline entry point: silver validation.

Runs referential integrity checks between silver tables before gold build
scripts are allowed to run.
"""

import inspect
import os
import sys

from pyspark.sql import SparkSession

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(inspect.currentframe().f_code.co_filename), "..", "..")))

from src.silver.referential_integrity import assert_no_orphans


def main():
    spark = SparkSession.builder.getOrCreate()

    orders = spark.table("olist.silver.orders")
    customers = spark.table("olist.silver.customers")
    order_items = spark.table("olist.silver.order_items")
    products = spark.table("olist.silver.products")

    assert_no_orphans(
        child_df=orders,
        parent_df=customers,
        child_key="customer_id",
        parent_key="customer_id",
        label="orders -> customers",
    )

    assert_no_orphans(
        child_df=order_items,
        parent_df=orders,
        child_key="order_id",
        parent_key="order_id",
        label="order_items -> orders",
    )

    assert_no_orphans(
        child_df=order_items,
        parent_df=products,
        child_key="product_id",
        parent_key="product_id",
        label="order_items -> products",
    )

    print("[silver_validation] no orphaned records found")

if __name__ == "__main__":
    main()
