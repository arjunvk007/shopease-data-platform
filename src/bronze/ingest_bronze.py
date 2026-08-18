"""
Bronze layer: raw ingestion for the ShopEase data platform.

Reads source files (orders, customers, products, clickstream events) and
lands them as Delta tables in the bronze schema with minimal transformation
-- just schema enforcement and ingestion metadata columns.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SOURCE_TABLES = ["orders", "customers", "products", "clickstream"]


def get_param(name, default=None):
    args = dict(arg.split("=", 1) for arg in sys.argv[1:] if "=" in arg)
    value = args.get(name, default)
    if value is None:
        raise ValueError(f"Missing required parameter: {name}")
    return value


def ingest_table(spark, table_name, catalog, schema, source_path):
    raw_df = (
        spark.read.format("json")
        .option("multiLine", "true")
        .load(f"{source_path}/{table_name}/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

    target = f"{catalog}.{schema}.{table_name}"
    raw_df.write.format("delta").mode("append").saveAsTable(target)
    print(f"Ingested {raw_df.count()} rows into {target}")


def main():
    spark = SparkSession.builder.getOrCreate()

    catalog = get_param("catalog", "shopease")
    schema = get_param("schema", "bronze")
    source_path = get_param("source_path", "/Volumes/shopease/landing/raw")

    for table_name in SOURCE_TABLES:
        ingest_table(spark, table_name, catalog, schema, source_path)


if __name__ == "__main__":
    main()
