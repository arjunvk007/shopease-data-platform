"""
Reusable bronze-layer ingestion framework for the ShopEase data platform.

Pipeline scripts under src/pipeline/ call into this module rather than
implementing Auto Loader / batch ingestion logic inline.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """Attach standard bronze ingestion metadata columns."""
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


def ingest_source_table(
    spark,
    source_path,
    target_table,
    file_format="json",
):
    """Batch-ingest a single source directory into a bronze Delta table."""

    df = (
        spark.read
        .format(file_format)
        .option("multiLine", "true")
        .load(source_path)
    )

    df = add_ingestion_metadata(df)

    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(target_table)
    )

    return df.count()


def ingest_source_table_streaming(
    spark,
    source_path,
    target_table,
    checkpoint_path,
    schema_location,
    file_format="csv",
):
    """Incrementally ingest a source directory using Auto Loader."""

    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", schema_location)
    )

    if file_format.lower() == "csv":
        reader = (
            reader
            .option("header", "true")
            .option("inferColumnTypes", "true")
        )

    df = reader.load(source_path)

    df = add_ingestion_metadata(df)

    query = (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    query.awaitTermination()

    return query


def row_counts(spark, table_name):
    """Return the current row count for a bronze table."""
    return spark.table(table_name).count()
