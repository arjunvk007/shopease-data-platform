"""
Pipeline entry point: gold validation.

Sanity-checks the published gold tables before the KPI refresh step runs.
"""

from pyspark.sql import SparkSession

GOLD_TABLES = [
    "olist.gold.dim_customer",
    "olist.gold.dim_product",
    "olist.gold.dim_date",
    "olist.gold.fact_sales",
    "olist.gold.fact_orders",
]

def main():
    spark = SparkSession.builder.getOrCreate()

    failures = []
    for table_name in GOLD_TABLES:
        count = spark.table(table_name).count()
        print(f"[gold_validation] {table_name}: {count} rows")
        if count == 0:
            failures.append(table_name)

    if failures:
        raise ValueError(f"Gold validation failed, empty tables: {failures}")

if __name__ == "__main__":
    main()
