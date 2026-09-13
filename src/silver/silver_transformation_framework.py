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


def split_by_referential_integrity(df: DataFrame, ref_df: DataFrame, key_column: str, ref_key_column: str = None):
    """Split df into (valid, orphaned) based on whether key_column has a match
    in ref_df's ref_key_column (defaults to the same name as key_column).

    Used to keep referential-integrity violations out of the silver layer
    without hard-failing the whole run: callers merge the valid half into the
    normal target table and route the orphaned half to a quarantine table
    instead, so a handful of bad upstream rows doesn't block everything else.
    """
    ref_key_column = ref_key_column or key_column
    ref_keys = ref_df.select(F.col(ref_key_column).alias(key_column)).distinct()
    valid = df.join(ref_keys, on=key_column, how="left_semi")
    orphaned = df.join(ref_keys, on=key_column, how="left_anti")
    return valid, orphaned
