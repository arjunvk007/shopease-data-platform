"""
Pipeline entry point: bronze ingestion.

Reads configuration, ingests each ShopEase source table into the bronze
schema, and logs the outcome. Reusable ingestion logic lives in
src/bronze/bronze_ingestion_framework.py.
"""

from pyspark.sql import SparkSession

import inspect
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(inspect.currentframe().f_code.co_filename), "..", "..")))

from src.bronze.bronze_ingestion_framework import ingest_source_table

SOURCE_ROOT = "/Volumes/shopease/landing/raw"
SOURCE_TABLES = {
    "orders": "shopease.bronze.orders",
    "customers": "shopease.bronze.customers",
    "products": "shopease.bronze.products",
    "clickstream": "shopease.bronze.clickstream",
}

def main():
    spark = SparkSession.builder.getOrCreate()

    for table_name, target_table in SOURCE_TABLES.items():
        source_path = f"{SOURCE_ROOT}/{table_name}/"
        row_count = ingest_source_table(spark, source_path, target_table)
        print(f"[bronze_ingestion] {target_table}: ingested {row_count} rows")

if __name__ == "__main__":
    main()
