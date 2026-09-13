"""
Pipeline entry point: KPI refresh.

Recomputes headline business KPIs from the gold layer and publishes them as
a small summary table for dashboards.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = SparkSession.builder.getOrCreate()

    sales_fact = spark.table("olist.gold.fact_sales")

    kpis = sales_fact.agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("total_customers"),
        F.sum(F.col("price") + F.col("freight_value")).alias("total_revenue"),
    )

    # Fully recomputed every run -- allow the overwrite to replace the
    # table schema too (see the matching comment in 06_gold_facts.py).
    kpis.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable("olist.gold.kpi_summary")
    print("[kpi_refresh] published olist.gold.kpi_summary")

if __name__ == "__main__":
    main()
