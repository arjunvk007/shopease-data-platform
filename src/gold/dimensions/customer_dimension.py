"""
Customer dimension for the ShopEase gold layer.
"""

def build_customer_dimension(spark):
        customers = spark.table("olist.silver.customers")
        return customers.select(
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state",
        )
