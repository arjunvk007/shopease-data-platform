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
    """Upsert source_df into target_table using the given merge condition."""
    target = DeltaTable.forName(spark, target_table)
    (
        target.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
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
