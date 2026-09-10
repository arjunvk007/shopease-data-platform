"""
Pipeline entry point: Bronze ingestion.

Incrementally ingests Olist CSV datasets from the S3 landing layer
into Unity Catalog Bronze Delta tables using Databricks Auto Loader.

Each dataset uses an independent checkpoint so file-processing state
is isolated per source.
"""

from pyspark.sql import SparkSession

import inspect
import os
import sys

# Allow imports from project root
sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(inspect.currentframe().f_code.co_filename),
            "..",
            "..",
        )
    )
)

from src.bronze.bronze_ingestion_framework import (
    ingest_source_table_streaming,
)


# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

SOURCE_ROOT = "s3://shopease-olist/landing"

CHECKPOINT_ROOT = "s3://shopease-olist/checkpoints/bronze"

SOURCE_TABLES = {
    "customers": "olist.bronze.customers",
    "orders": "olist.bronze.orders",
    "order_items": "olist.bronze.order_items",
    "order_payments": "olist.bronze.order_payments",
    "order_reviews": "olist.bronze.order_reviews",
    "products": "olist.bronze.products",
    "sellers": "olist.bronze.sellers",
    "geolocation": "olist.bronze.geolocation",
    "category_translation": "olist.bronze.category_translation",
}


def main():
    spark = SparkSession.builder.getOrCreate()

    queries = []

    for dataset_name, target_table in SOURCE_TABLES.items():

        source_path = f"{SOURCE_ROOT}/{dataset_name}/"
        checkpoint_path = (
            f"{CHECKPOINT_ROOT}/{dataset_name}/"
        )

        print("=" * 80)
        print(f"[bronze_ingestion] Dataset      : {dataset_name}")
        print(f"[bronze_ingestion] Source       : {source_path}")
        print(f"[bronze_ingestion] Target       : {target_table}")
        print(f"[bronze_ingestion] Checkpoint   : {checkpoint_path}")

        query = ingest_source_table_streaming(
            spark=spark,
            source_path=source_path,
            target_table=target_table,
            checkpoint_path=checkpoint_path,
            file_format="csv",
        )

        queries.append((dataset_name, query))

    # availableNow=True causes each stream to terminate
    # after all currently available files have been processed.
    for dataset_name, query in queries:
        query.awaitTermination()
        print(
            f"[bronze_ingestion] {dataset_name}: ingestion completed"
        )

    print("=" * 80)
    print("[bronze_ingestion] All Bronze datasets completed successfully.")


if __name__ == "__main__":
    main()