"""
Customer dimension for the ShopEase gold layer.
"""

def build_customer_dimension(spark):
    customers = spark.table("silver.customers")
    return customers.select(
        "customer_id",
        "customer_name",
        "country",
        "customer_segment",
    )
