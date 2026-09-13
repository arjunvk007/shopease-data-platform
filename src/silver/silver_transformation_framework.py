"""
Reusable silver-layer transformation framework for the ShopEase data platform.

Provides generic incremental merge and schema-conformance helpers used by the
orchestration scripts under src/pipeline/.
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def merge_incremental(spark, source_df: DataFrame, target_table: str, merge_condition: str):
    """Upsert source_df into target_table using the given merge condition.

    If target_table does not exist yet, it is created from source_df instead
    (a plain MERGE INTO cannot create a new table), so callers can point this
    at a brand-new silver table on its first run.
    """
    if not spark.catalog.tableExists(target_table):
        source_df.write.format("delta").saveAsTable(target_table)
        return

    target = DeltaTable.forName(spark, target_table)
    # Update/insert only the columns present in source_df. Using the blanket
    # *All variants instead would require target_table's schema to match
    # source_df exactly, which breaks the moment the target has picked up an
    # extra column (e.g. Auto Loader's _rescued_data) that the source select
    # doesn't include.
    column_map = {column: f"source.{column}" for column in source_df.columns}
    (
        target.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdate(set=column_map)
        .whenNotMatchedInsert(values=column_map)
        .execute()
    )


def dedupe_latest(df: DataFrame, key_columns, order_column: str) -> DataFrame:
    """Keep only the latest row per key, ordered by order_column descending."""
    window = Window.partitionBy(*key_columns).orderBy(F.col(order_column).desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def enforce_not_null(df: DataFrame, required_columns) -> DataFrame:
    """Drop rows that are missing any of the required columns."""
    condition = None
    for column in required_columns:
        clause = F.col(column).isNotNull()
        condition = clause if condition is None else condition & clause
    return df.filter(condition)
