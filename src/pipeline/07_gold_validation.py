"""
Pipeline entry point: gold validation.

Sanity-checks the published gold tables before the KPI refresh step runs.
"""

from pyspark.sql import SparkSession

from src.pipeline.runtime_config import get_runtime_config


def main():
    spark = SparkSession.builder.getOrCreate()

    catalog, environment = get_runtime_config()

    gold_tables = [
        f"{catalog}.gold.dim_customer",
        f"{catalog}.gold.dim_product",
        f"{catalog}.gold.dim_date",
        f"{catalog}.gold.fact_sales",
        f"{catalog}.gold.fact_orders",
    ]

    print("=" * 70)
    print("ShopEase Gold Validation")
    print(f"Environment : {environment}")
    print(f"Catalog     : {catalog}")
    print("=" * 70)

    failures = []

    for table_name in gold_tables:
        count = spark.table(table_name).count()

        print(
            f"[gold_validation] "
            f"{table_name}: {count} rows"
        )

        if count == 0:
            failures.append(table_name)

    if failures:
        raise ValueError(
            f"Gold validation failed, empty tables: {failures}"
        )

    print("=" * 70)
    print("GOLD VALIDATION PASSED")
    print("=" * 70)


if __name__ == "__main__":
    main()