"""
Pipeline entry point: KPI refresh.

Recomputes headline business KPIs from the gold layer and publishes them as
a small summary table for dashboards.
"""

import inspect
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(inspect.currentframe().f_code.co_filename),
            "..",
            "..",
        )
    )
)

from src.pipeline.runtime_config import get_runtime_config


def main():
    spark = SparkSession.builder.getOrCreate()

    catalog, environment = get_runtime_config()

    print("=" * 70)
    print("ShopEase KPI Refresh")
    print(f"Environment : {environment}")
    print(f"Catalog     : {catalog}")
    print("=" * 70)

    sales_fact = spark.table(f"{catalog}.gold.fact_sales")

    kpis = sales_fact.agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("total_customers"),
        F.sum(
            F.col("price") + F.col("freight_value")
        ).alias("total_revenue"),
    )

    (
        kpis.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.gold.kpi_summary")
    )

    print(
        f"[kpi_refresh] published "
        f"{catalog}.gold.kpi_summary"
    )

    print("=" * 70)
    print("KPI REFRESH COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
