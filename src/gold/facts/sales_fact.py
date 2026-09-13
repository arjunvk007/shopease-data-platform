"""
Sales fact table for the ShopEase gold layer.

Grain: one row per order line (order_items).
"""

def build_sales_fact(spark):
        orders = spark.table("olist.silver.orders")
        order_items = spark.table("olist.silver.order_items")
        return (
            order_items.join(orders, "order_id")
            .select(
                "order_id",
                "customer_id",
                "product_id",
                "order_date",
                "price",
                "freight_value",
            )
        )
