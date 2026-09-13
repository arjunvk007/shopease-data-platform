"""
Pipeline entry point: silver incremental processing.

Reads the latest bronze rows and merges them into the silver tables using
the reusable incremental-merge helper. Processes customers, products,
orders, and order_items so downstream validation and gold-layer steps have
the tables they need.
"""

import inspect
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(inspect.currentframe().f_code.co_filename), "..", "..")))

from src.silver.silver_transformation_framework import (
    dedupe_latest,
    enforce_not_null,
    merge_incremental,
    remove_from_target,
    split_by_referential_integrity,
)


def main():
    spark = SparkSession.builder.getOrCreate()

    # ---- customers ----
    customers = spark.table("olist.bronze.customers")
    customers = enforce_not_null(customers, ["customer_id"])
    customers = dedupe_latest(customers, ["customer_id"], "_ingested_at")
    customers = customers.select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    )
    merge_incremental(
        spark,
        source_df=customers,
        target_table="olist.silver.customers",
        merge_condition="target.customer_id = source.customer_id",
    )
    print("[silver_incremental_processing] merged customers into olist.silver.customers")

    # ---- products ----
    products = spark.table("olist.bronze.products")
    products = enforce_not_null(products, ["product_id"])
    products = dedupe_latest(products, ["product_id"], "_ingested_at")
    translation = spark.table("olist.bronze.category_translation")
    products = products.join(translation, on="product_category_name", how="left")
    products = products.select(
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    )
    merge_incremental(
        spark,
        source_df=products,
        target_table="olist.silver.products",
        merge_condition="target.product_id = source.product_id",
    )
    print("[silver_incremental_processing] merged products into olist.silver.products")

    # ---- orders ----
    # order_purchase_timestamp in the landing data is not consistently one
    # format: some rows are ISO ("2018-01-14 14:33:31"), others are
    # "dd-MM-yyyy HH:mm" (e.g. "11-09-2018 08:53"). Try each explicit format
    # in turn and keep whichever one parses, instead of assuming a single
    # format and silently failing (or nulling out) the rows that don't match.
    orders = spark.table("olist.bronze.orders")
    orders = enforce_not_null(orders, ["order_id", "customer_id"])
    orders = dedupe_latest(orders, ["order_id"], "_ingested_at")
    orders = orders.withColumn(
        "order_date",
        F.coalesce(
            F.try_to_timestamp("order_purchase_timestamp", F.lit("yyyy-MM-dd HH:mm:ss")),
            F.try_to_timestamp("order_purchase_timestamp", F.lit("dd-MM-yyyy HH:mm")),
        ).cast("date"),
    )
    orders = orders.select(
        "order_id",
        "customer_id",
        "order_status",
        "order_date",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    )

    # A handful of orders reference a customer_id that isn't in
    # olist.silver.customers (a gap in the landing data, not something this
    # pipeline can fix). Rather than let that hard-fail silver_validation and
    # block every downstream task, route those orders to a quarantine table
    # and keep the rest of the pipeline moving on the clean data.
    valid_orders, orphaned_orders = split_by_referential_integrity(
        orders, customers, "customer_id"
    )
    orphaned_orders_count = orphaned_orders.count()
    if orphaned_orders_count > 0:
        print(
            f"[silver_incremental_processing] quarantining {orphaned_orders_count} "
            "orders with no matching customer"
        )
        merge_incremental(
            spark,
            source_df=orphaned_orders.withColumn("_quarantine_reason", F.lit("missing customer")),
            target_table="olist.quarantine.orders",
            merge_condition="target.order_id = source.order_id",
        )
        # merge_incremental only upserts, so if an earlier (pre-quarantine)
        # run already wrote these order_ids into olist.silver.orders, they'd
        # otherwise stay there forever even though they're excluded from
        # valid_orders below. Purge them so the silver table only ever holds
        # orders with a real customer.
        remove_from_target(
            spark,
            keys_df=orphaned_orders.select("order_id"),
            target_table="olist.silver.orders",
            delete_condition="target.order_id = source.order_id",
        )

    merge_incremental(
        spark,
        source_df=valid_orders,
        target_table="olist.silver.orders",
        merge_condition="target.order_id = source.order_id",
    )
    print("[silver_incremental_processing] merged orders into olist.silver.orders")

    # ---- order_items ----
    order_items = spark.table("olist.bronze.order_items")
    order_items = enforce_not_null(order_items, ["order_id", "order_item_id", "product_id"])
    order_items = dedupe_latest(order_items, ["order_id", "order_item_id"], "_ingested_at")
    order_items = order_items.select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "price",
        "freight_value",
    )

    # Cascade the same quarantine treatment: an order_item can only be valid
    # if it points at an order that made it into olist.silver.orders (i.e.
    # wasn't itself quarantined above) AND at a real product.
    valid_by_order, orphaned_by_order = split_by_referential_integrity(
        order_items, valid_orders, "order_id"
    )
    valid_order_items, orphaned_by_product = split_by_referential_integrity(
        valid_by_order, products, "product_id"
    )
    orphaned_order_items = orphaned_by_order.withColumn(
        "_quarantine_reason", F.lit("missing order")
    ).unionByName(
        orphaned_by_product.withColumn("_quarantine_reason", F.lit("missing product"))
    )
    orphaned_order_items_count = orphaned_order_items.count()
    if orphaned_order_items_count > 0:
        print(
            f"[silver_incremental_processing] quarantining {orphaned_order_items_count} "
            "order_items with no matching order or product"
        )
        merge_incremental(
            spark,
            source_df=orphaned_order_items,
            target_table="olist.quarantine.order_items",
            merge_condition=(
                "target.order_id = source.order_id "
                "AND target.order_item_id = source.order_item_id"
            ),
        )
        # Same retroactive cleanup as orders above: purge any of these
        # order_id/order_item_id pairs that an earlier (pre-cascading-check)
        # run already wrote into olist.silver.order_items, since
        # merge_incremental alone would leave them there indefinitely.
        remove_from_target(
            spark,
            keys_df=orphaned_order_items.select("order_id", "order_item_id"),
            target_table="olist.silver.order_items",
            delete_condition=(
                "target.order_id = source.order_id "
                "AND target.order_item_id = source.order_item_id"
            ),
        )

    merge_incremental(
        spark,
        source_df=valid_order_items,
        target_table="olist.silver.order_items",
        merge_condition=(
            "target.order_id = source.order_id "
            "AND target.order_item_id = source.order_item_id"
        ),
    )
    print("[silver_incremental_processing] merged order_items into olist.silver.order_items")

if __name__ == "__main__":
    main()
