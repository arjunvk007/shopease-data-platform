"""
Pipeline entry point: KPI refresh.

Recomputes headline business KPIs from the gold layer and publishes them as
a small summary table for dashboards.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder.getOrCreate()

    sales_fact = spark.table("shopease.gold.fact_sales")

    kpis = sales_fact.agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("total_customers"),
        F.sum(F.col("quantity") * F.col("unit_price")).alias("total_revenue"),
    )

    kpis.write.format("delta").mode("overwrite").saveAsTable("shopease.gold.kpi_summary")
    print("[kpi_refresh] published shopease.gold.kpi_summary")

if __name__ == "__main__":
    main()
