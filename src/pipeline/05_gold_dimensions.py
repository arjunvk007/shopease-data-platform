"""
Pipeline entry point: gold dimension build.

Builds and publishes each gold dimension table. Business logic for each
dimension lives under src/gold/dimensions/.
"""

import inspect
import os
import sys

from pyspark.sql import SparkSession

sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(inspect.currentframe().f_code.co_filename),
            "..",
            "..",
        )
    )
)

from src.gold.dimensions.customer_dimension import build_customer_dimension
from src.gold.dimensions.date_dimension import build_date_dimension
from src.gold.dimensions.product_dimension import build_product_dimension
from src.pipeline.runtime_config import get_runtime_config


def main():
    spark = SparkSession.builder.getOrCreate()

    catalog, environment = get_runtime_config()

    print("=" * 70)
    print("ShopEase Gold Dimension Build")
    print(f"Environment : {environment}")
    print(f"Catalog     : {catalog}")
    print("=" * 70)

    # Gold dimension tables are fully recomputed every run, so allow
    # overwrite to replace the table schema when necessary.

    # ---------------------------------------------------------------
    # Customer dimension
    # ---------------------------------------------------------------
    customer_dim = build_customer_dimension(spark)

    (
        customer_dim.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.gold.dim_customer")
    )

    print(
        f"[gold_dimensions] published "
        f"{catalog}.gold.dim_customer"
    )

    # ---------------------------------------------------------------
    # Product dimension
    # ---------------------------------------------------------------
    product_dim = build_product_dimension(spark)

    (
        product_dim.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.gold.dim_product")
    )

    print(
        f"[gold_dimensions] published "
        f"{catalog}.gold.dim_product"
    )

    # ---------------------------------------------------------------
    # Date dimension
    # ---------------------------------------------------------------
    date_dim = build_date_dimension(
        spark,
        "2024-01-01",
        "2027-12-31",
    )

    (
        date_dim.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.gold.dim_date")
    )

    print(
        f"[gold_dimensions] published "
        f"{catalog}.gold.dim_date"
    )

    print("=" * 70)
    print("GOLD DIMENSION BUILD COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()