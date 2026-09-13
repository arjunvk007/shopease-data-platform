"""
Unit tests for the ShopEase silver/gold transformation logic.

These tests use a local PySpark session and small in-memory DataFrames so
they can run in CI without a Databricks workspace.
"""

import pytest
from pyspark.sql import SparkSession

from src.gold.dimensions.customer_dimension import build_customer_dimension
from src.gold.facts.sales_fact import build_sales_fact
from src.silver.referential_integrity import find_orphan_records


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
                ("c1", "u1"),
                ("c2", "u2"),
            ],
            ["customer_id", "customer_unique_id"],
        )

    orphans = find_orphan_records(orders, customers, "customer_id", "customer_id")
    orphan_ids = {row["order_id"] for row in orphans.collect()}

    assert orphan_ids == {"o3"}


def test_build_customer_dimension(spark, monkeypatch):
        customers = spark.createDataFrame(
                    [
                                    ("c1", "u1", "01310-100", "sao paulo", "SP"),
                                    ("c2", "u2", "20040-020", "rio de janeiro", "RJ"),
                    ],
                    [
                                    "customer_id",
                                    "customer_unique_id",
                                    "customer_zip_code_prefix",
                                    "customer_city",
                                    "customer_state",
                    ],
        )
        monkeypatch.setattr(spark, "table", lambda name: customers, raising=False)

    result = build_customer_dimension(spark)
    rows = {row["customer_id"]: row for row in result.collect()}

    assert set(result.columns) == {
                "customer_id",
                "customer_unique_id",
                "customer_city",
                "customer_state",
    }
    assert rows["c1"]["customer_state"] == "SP"
    assert rows["c2"]["customer_city"] == "rio de janeiro"


def test_build_sales_fact(spark, monkeypatch):
        orders = spark.createDataFrame(
                    [
                                    ("o1", "c1", "2026-01-01"),
                                    ("o2", "c2", "2026-01-02"),
                    ],
                    ["order_id", "customer_id", "order_date"],
        )
        order_items = spark.createDataFrame(
            [
                ("o1", "p1", 10.0, 2.0),
                ("o1", "p2", 20.0, 3.0),
                ("o2", "p1", 10.0, 2.0),
            ],
            ["order_id", "product_id", "price", "freight_value"],
        )

    tables = {"olist.silver.orders": orders, "olist.silver.order_items": order_items}
    monkeypatch.setattr(spark, "table", lambda name: tables[name], raising=False)

    result = build_sales_fact(spark)
    rows = result.collect()

    assert len(rows) == 3
    assert set(result.columns) == {
                "order_id",
                "customer_id",
                "product_id",
                "order_date",
                "price",
                "freight_value",
    }
