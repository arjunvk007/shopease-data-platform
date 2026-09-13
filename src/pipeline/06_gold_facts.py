"""
Pipeline entry point: gold fact build.

Builds and publishes each gold fact table. Business logic for each fact
lives under src/gold/facts/.
"""

import inspect
import os
import sys

from pyspark.sql import SparkSession

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(inspect.currentframe().f_code.co_filename), "..", "..")))

from src.gold.facts.order_fact import build_order_fact
from src.gold.facts.sales_fact import build_sales_fact


def main():
    spark = SparkSession.builder.getOrCreate()

    # Gold fact tables are fully recomputed from the silver layer on every
    # run, so the write is a full overwrite rather than an incremental
    # merge. overwriteSchema=true lets that overwrite also replace the
    # table's schema -- without it, Delta blocks the write with
    # DELTA_METADATA_MISMATCH the moment a fact table's column set changes
    # (as happened here: fact_sales previously had quantity/unit_price from
    # before the real-Olist-schema refactor, but build_sales_fact now
    # produces price/freight_value).
    sales_fact = build_sales_fact(spark)
    sales_fact.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable("olist.gold.fact_sales")

    order_fact = build_order_fact(spark)
    order_fact.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable("olist.gold.fact_orders")

    print("[gold_facts] published fact_sales, fact_orders")

if __name__ == "__main__":
    main()
