"""
Order fact table for the ShopEase gold layer.

Grain: one row per order, aggregated from order lines, complementing the
line-level detail in sales_fact.py.
"""

from pyspark.sql import functions as F


def build_order_fact(spark):
        orders = spark.table("olist.silver.orders")
        order_items = spark.table("olist.silver.order_items")

    line_agg = order_items.groupBy("order_id").agg(
                F.count("product_id").alias("line_count"),
                F.sum(F.col("price") + F.col("freight_value")).alias("order_total"),
    )

    return orders.join(line_agg, "order_id", "left").select(
                "order_id",
                "customer_id",
                "order_date",
                "line_count",
                "order_total",
    )
