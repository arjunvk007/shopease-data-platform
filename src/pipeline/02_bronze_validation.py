"""
Pipeline entry point: bronze validation.

Confirms that each bronze table received rows in the latest ingestion run
and fails fast if a source came back empty.
"""

import inspect
import os
import sys

from pyspark.sql import SparkSession

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(inspect.currentframe().f_code.co_filename), "..", "..")))

from src.bronze.bronze_ingestion_framework import row_counts

BRONZE_TABLES = [
    "olist.bronze.customers",
    "olist.bronze.orders",
    "olist.bronze.order_items",
    "olist.bronze.order_payments",
    "olist.bronze.order_reviews",
    "olist.bronze.products",
    "olist.bronze.sellers",
    "olist.bronze.geolocation",
    "olist.bronze.category_translation",
]

def main():
    spark = SparkSession.builder.getOrCreate()

    failures = []
    for table_name in BRONZE_TABLES:
        count = row_counts(spark, table_name)
        print(f"[bronze_validation] {table_name}: {count} rows")
        if count == 0:
            failures.append(table_name)

    if failures:
        raise ValueError(f"Bronze validation failed, empty tables: {failures}")

if __name__ == "__main__":
    main()
