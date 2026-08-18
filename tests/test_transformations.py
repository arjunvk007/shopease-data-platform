"""
Unit tests for the ShopEase silver/gold transformation logic.

These tests use a local PySpark session and small in-memory DataFrames so
they can run in CI without a Databricks workspace.
"""

import pytest
from pyspark.sql import SparkSession

from src.silver.referential_integrity import find_orphan_records
from src.gold.dimensions.customer_dimension import build_customer_dimension
from src.gold.facts.sales_fact import build_sales_fact


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("shopease-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_find_orphan_records(spark):
    orders = spark.createDataFrame(
        [
            ("o1", "c1"),
            ("o2", "c2"),
            ("o3", "c99"),
        ],
        ["order_id", "customer_id"],
    )
    customers = spark.createDataFrame(
        [
            ("c1", "Alice"),
            ("c2", "Bob"),
        ],
        ["customer_id", "customer_name"],
    )

    orphans = find_orphan_records(orders, customers, "customer_id", "customer_id")
    orphan_ids = {row["order_id"] for row in orphans.collect()}

    assert orphan_ids == {"o3"}


def test_build_customer_dimension(spark, monkeypatch):
    customers = spark.createDataFrame(
        [
            ("c1", "Alice", "US", "Consumer"),
            ("c2", "Bob", "CA", "Corporate"),
        ],
        ["customer_id", "customer_name", "country", "customer_segment"],
    )
    monkeypatch.setattr(spark, "table", lambda name: customers, raising=False)

    result = build_customer_dimension(spark)
    rows = {row["customer_id"]: row for row in result.collect()}

    assert set(result.columns) == {"customer_id", "customer_name", "country", "customer_segment"}
    assert rows["c1"]["country"] == "US"
    assert rows["c2"]["customer_segment"] == "Corporate"


def test_build_sales_fact(spark, monkeypatch):
    orders = spark.createDataFrame(
        [
            ("o1", "c1", "2026-01-01"),
            ("o2", "c2", "2026-01-02"),
        ],
        ["order_id", "customer_id", "order_date"],
    )
    order_lines = spark.createDataFrame(
        [
            ("o1", "p1", 2, 10.0),
            ("o1", "p2", 1, 20.0),
            ("o2", "p1", 3, 10.0),
        ],
        ["order_id", "product_id", "quantity", "unit_price"],
    )

    tables = {"silver.orders": orders, "silver.order_lines": order_lines}
    monkeypatch.setattr(spark, "table", lambda name: tables[name], raising=False)

    result = build_sales_fact(spark)
    rows = result.collect()

    assert len(rows) == 3
    assert set(result.columns) == {
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "quantity",
        "unit_price",
    }
