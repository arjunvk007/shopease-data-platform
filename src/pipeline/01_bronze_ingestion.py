from pyspark.sql import SparkSession

from src.bronze.bronze_ingestion_framework import ingest_source_table_streaming

spark = SparkSession.builder.getOrCreate()


# -------------------------------------------------------------------
# ShopEase Bronze configuration
# -------------------------------------------------------------------

SOURCE_ROOT = "s3://shopease-olist/landing"
CHECKPOINT_ROOT = "s3://shopease-olist/checkpoints/bronze"
SCHEMA_ROOT = "s3://shopease-olist/checkpoints/schema"

CATALOG = "olist"
BRONZE_SCHEMA = "bronze"


DATASETS = [
    "customers",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "geolocation",
    "category_translation",
]


# -------------------------------------------------------------------
# Bronze ingestion
# -------------------------------------------------------------------

for dataset in DATASETS:

    source_path = f"{SOURCE_ROOT}/{dataset}"

    target_table = f"{CATALOG}.{BRONZE_SCHEMA}.{dataset}"

    checkpoint_path = f"{CHECKPOINT_ROOT}/{dataset}"

    schema_location = f"{SCHEMA_ROOT}/{dataset}"

    print(f"Starting Bronze ingestion for: {dataset}")
    print(f"Source: {source_path}")
    print(f"Target: {target_table}")
    print(f"Checkpoint: {checkpoint_path}")
    print(f"Schema location: {schema_location}")

    ingest_source_table_streaming(
        spark=spark,
        source_path=source_path,
        target_table=target_table,
        checkpoint_path=checkpoint_path,
        schema_location=schema_location,
        file_format="csv",
    )

    print(f"Completed Bronze ingestion for: {dataset}")