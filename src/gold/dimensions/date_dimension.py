"""
Date dimension for the ShopEase gold layer.

Unlike the other dimensions this one is generated rather than sourced from a
silver table, so it can be rebuilt independently of any upstream data.
"""

from pyspark.sql import functions as F

def build_date_dimension(spark, start_date: str, end_date: str):
    dates = spark.sql(
        f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS calendar_date"
    )
    return dates.select(
        F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),
        "calendar_date",
        F.year("calendar_date").alias("year"),
        F.quarter("calendar_date").alias("quarter"),
        F.month("calendar_date").alias("month"),
        F.dayofmonth("calendar_date").alias("day_of_month"),
        F.date_format("calendar_date", "EEEE").alias("day_name"),
    )
