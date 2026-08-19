"""
Pipeline entry point: gold fact build.

Builds and publishes each gold fact table. Business logic for each fact
lives under src/gold/facts/.
"""

from pyspark.sql import SparkSession

import inspect
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(inspect.currentframe().f_code.co_filename), "..", "..")))

from src.gold.facts.sales_fact import build_sales_fact
from src.gold.facts.order_fact import build_order_fact

def main():
    spark = SparkSession.builder.getOrCreate()

    sales_fact = build_sales_fact(spark)
    sales_fact.write.format("delta").mode("overwrite").saveAsTable("olist.gold.fact_sales")

    order_fact = build_order_fact(spark)
    order_fact.write.format("delta").mode("overwrite").saveAsTable("olist.gold.fact_orders")

    print("[gold_facts] published fact_sales, fact_orders")

if __name__ == "__main__":
    main()
