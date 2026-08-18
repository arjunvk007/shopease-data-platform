"""
Pipeline entry point: gold validation.

Sanity-checks the published gold tables before the KPI refresh step runs.
"""

from pyspark.sql import SparkSession

GOLD_TABLES = [
    "shopease.gold.dim_customer",
    "shopease.gold.dim_product",
    "shopease.gold.dim_date",
    "shopease.gold.fact_sales",
    "shopease.gold.fact_orders",
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
