"""
Sales fact table for the ShopEase gold layer.

Grain: one row per order line.
"""

def build_sales_fact(spark):
    orders = spark.table("silver.orders")
    order_lines = spark.table("silver.order_lines")
    return (
        order_lines.join(orders, "order_id")
        .select(
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "quantity",
            "unit_price",
        )
    )
