"""
Pipeline entry point: gold fact build.

Builds and publishes each gold fact table. Business logic for each fact
lives under src/gold/facts/.
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

from src.gold.facts.order_fact import build_order_fact
from src.gold.facts.sales_fact import build_sales_fact
from src.pipeline.runtime_config import get_runtime_config


def main():
    spark = SparkSession.builder.getOrCreate()

    catalog, environment = get_runtime_config()

    print("=" * 70)
    print("ShopEase Gold Fact Build")
    print(f"Environment : {environment}")
    print(f"Catalog     : {catalog}")
    print("=" * 70)

    # Gold fact tables are fully recomputed from the Silver layer on every
    # run, so the write is a full overwrite rather than an incremental
    # merge. overwriteSchema=true allows the table schema to evolve when
    # the fact definition changes.

    # ---------------------------------------------------------------
    # Sales fact
    # ---------------------------------------------------------------
    sales_fact = build_sales_fact(spark)

    (
        sales_fact.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.gold.fact_sales")
    )

    print(
        f"[gold_facts] published "
        f"{catalog}.gold.fact_sales"
    )

    # ---------------------------------------------------------------
    # Order fact
    # ---------------------------------------------------------------
    order_fact = build_order_fact(spark)

    (
        order_fact.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.gold.fact_orders")
    )

    print(
        f"[gold_facts] published "
        f"{catalog}.gold.fact_orders"
    )

    print("=" * 70)
    print("GOLD FACT BUILD COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()