"""
Pipeline entry point: gold dimension build.

Builds and publishes each gold dimension table. Business logic for each
dimension lives under src/gold/dimensions/.
"""

from pyspark.sql import SparkSession

import os
import sys
sys.path.append(os.path.abspath("../.."))

from src.gold.dimensions.customer_dimension import build_customer_dimension
from src.gold.dimensions.product_dimension import build_product_dimension
from src.gold.dimensions.date_dimension import build_date_dimension

def main():
    spark = SparkSession.builder.getOrCreate()

    customer_dim = build_customer_dimension(spark)
    customer_dim.write.format("delta").mode("overwrite").saveAsTable("shopease.gold.dim_customer")

    product_dim = build_product_dimension(spark)
    product_dim.write.format("delta").mode("overwrite").saveAsTable("shopease.gold.dim_product")

    date_dim = build_date_dimension(spark, "2024-01-01", "2027-12-31")
    date_dim.write.format("delta").mode("overwrite").saveAsTable("shopease.gold.dim_date")

    print("[gold_dimensions] published dim_customer, dim_product, dim_date")

if __name__ == "__main__":
    main()
