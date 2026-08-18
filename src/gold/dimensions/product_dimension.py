"""
Product dimension for the ShopEase gold layer.
"""

def build_product_dimension(spark):
    products = spark.table("silver.products")
    return products.select(
        "product_id",
        "product_name",
        "category",
        "subcategory",
    )
