"""
Referential integrity checks for the ShopEase silver layer.

Used by the *_validation.py scripts under src/pipeline/ to catch orphaned
child records before they reach the gold layer.
"""

from pyspark.sql import DataFrame

def find_orphan_records(child_df: DataFrame, parent_df: DataFrame, child_key: str, parent_key: str) -> DataFrame:
    """Return rows in child_df whose key has no match in parent_df."""
    return child_df.alias("child").join(
        parent_df.alias("parent"),
        child_df[child_key] == parent_df[parent_key],
        "left_anti",
    )

def assert_no_orphans(child_df: DataFrame, parent_df: DataFrame, child_key: str, parent_key: str, label: str):
    """Raise if any orphaned records are found; used as a hard validation gate."""
    orphans = find_orphan_records(child_df, parent_df, child_key, parent_key)
    orphan_count = orphans.count()
    if orphan_count > 0:
        raise ValueError(f"{label}: found {orphan_count} orphaned record(s) on {child_key}")
    return True
