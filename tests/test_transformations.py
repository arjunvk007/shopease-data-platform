"""
Unit tests for the ShopEase silver/gold transformation logic.

These tests use a local PySpark session and small in-memory DataFrames so
they can run in CI without a Databricks workspace.
"""

import pytest
from pyspark.sql import SparkSession

from src.silver.transform_silver import clean_orders
from src.gold.build_gold import build_daily_sales


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("shopease-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def bronze_orders(spark):
    data = [
        ("o1", "c1", 100.0, "2026-01-01T10:00:00"),
        ("o1", "c1", 100.0, "2026-01-01T10:00:00"),
        ("o2", "c2", -5.0, "2026-01-02T09:00:00"),
        ("o3", "c3", 25.0, "2026-01-02T11:00:00"),
        (None, "c4", 40.0, "2026-01-03T08:00:00"),
    ]
    columns = ["order_id", "customer_id", "order_total", "order_timestamp"]
    return spark.createDataFrame(data, columns)


def test_clean_orders_dedupes_and_filters(spark, bronze_orders, monkeypatch):
    monkeypatch.setattr(spark, "table", lambda name: bronze_orders, raising=False)

    result = clean_orders(spark, "shopease", "bronze")
    order_ids = {row["order_id"] for row in result.collect()}

    assert "o1" in order_ids
    assert "o2" not in order_ids
    assert None not in order_ids
    assert result.filter(result.order_id == "o1").count() == 1


def test_build_daily_sales_aggregates(spark, monkeypatch):
    silver_orders = spark.createDataFrame(
        [
            ("o1", "c1", 100.0, "2026-01-01"),
            ("o2", "c2", 50.0, "2026-01-01"),
            ("o3", "c1", 75.0, "2026-01-02"),
        ],
        ["order_id", "customer_id", "order_total", "order_date"],
    )
    monkeypatch.setattr(spark, "table", lambda name: silver_orders, raising=False)

    result = build_daily_sales(spark, "shopease", "silver")
    rows = {row["order_date"]: row for row in result.collect()}

    assert rows["2026-01-01"]["order_count"] == 2
    assert rows["2026-01-01"]["gross_revenue"] == 150.0
    assert rows["2026-01-02"]["distinct_customers"] == 1
