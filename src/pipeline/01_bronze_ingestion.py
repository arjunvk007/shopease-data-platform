import inspect
import os
import sys

from pyspark.sql import SparkSession

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
from src.pipeline.runtime_config import get_runtime_config

spark = SparkSession.builder.getOrCreate()


# -------------------------------------------------------------------
# Runtime configuration
# -------------------------------------------------------------------

CATALOG, ENVIRONMENT = get_runtime_config()


# -------------------------------------------------------------------
# ShopEase Bronze configuration
# -------------------------------------------------------------------

SOURCE_ROOT = "s3://shopease-olist/landing"

CHECKPOINT_ROOT = (
    f"s3://shopease-olist/checkpoints/{ENVIRONMENT}/bronze"
)

SCHEMA_ROOT = (
    f"s3://shopease-olist/checkpoints/{ENVIRONMENT}/schema"
)

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

    print(f"Environment: {ENVIRONMENT}")
    print(f"Catalog: {CATALOG}")
    print(f"Starting Bronze ingestion: {dataset}")
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

    print(f"Completed Bronze ingestion: {dataset}")
