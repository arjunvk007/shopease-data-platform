"""
Product dimension for the ShopEase gold layer.
"""

def build_product_dimension(spark):
        products = spark.table("olist.silver.products")
        return products.select(
            "product_id",
            "product_category_name",
            "product_category_name_english",
            "product_weight_g",
        )
