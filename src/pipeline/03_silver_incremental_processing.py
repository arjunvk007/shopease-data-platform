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
from src.silver.silver_transformation_framework import (
    dedupe_latest,
    enforce_not_null,
    merge_incremental,
    remove_from_target,
    split_by_referential_integrity,
)


def main():
    spark = SparkSession.builder.getOrCreate()

    catalog, environment = get_runtime_config()

    print("=" * 70)
    print("ShopEase Silver Incremental Processing")
    print(f"Environment : {environment}")
    print(f"Catalog     : {catalog}")
    print("=" * 70)

    # -----------------------------------------------------------------
    # Customers
    # -----------------------------------------------------------------
    customers = spark.table(f"{catalog}.bronze.customers")
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
        target_table=f"{catalog}.silver.customers",
        merge_condition="target.customer_id = source.customer_id",
    )

    print(
        f"[silver_incremental_processing] merged customers into "
        f"{catalog}.silver.customers"
    )

    # -----------------------------------------------------------------
    # Products
    # -----------------------------------------------------------------
    products = spark.table(f"{catalog}.bronze.products")
    products = enforce_not_null(products, ["product_id"])
    products = dedupe_latest(products, ["product_id"], "_ingested_at")

    translation = spark.table(f"{catalog}.bronze.category_translation")
        translation = dedupe_latest(
                translation, ["product_category_name"], "_ingested_at"
        )

    products = products.join(
        translation,
        on="product_category_name",
        how="left",
    )

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
        target_table=f"{catalog}.silver.products",
        merge_condition="target.product_id = source.product_id",
    )

    print(
        f"[silver_incremental_processing] merged products into "
        f"{catalog}.silver.products"
    )

    # -----------------------------------------------------------------
    # Orders
    # -----------------------------------------------------------------
    # order_purchase_timestamp in the landing data is not consistently one
    # format. Try each explicit format and keep whichever parses.
    orders = spark.table(f"{catalog}.bronze.orders")

    orders = enforce_not_null(
        orders,
        ["order_id", "customer_id"],
    )

    orders = dedupe_latest(
        orders,
        ["order_id"],
        "_ingested_at",
    )

    orders = orders.withColumn(
        "order_date",
        F.coalesce(
            F.try_to_timestamp(
                "order_purchase_timestamp",
                F.lit("yyyy-MM-dd HH:mm:ss"),
            ),
            F.try_to_timestamp(
                "order_purchase_timestamp",
                F.lit("dd-MM-yyyy HH:mm"),
            ),
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

    # Orders without a valid customer are routed to quarantine.
    valid_orders, orphaned_orders = split_by_referential_integrity(
        orders,
        customers,
        "customer_id",
    )

    orphaned_orders_count = orphaned_orders.count()

    if orphaned_orders_count > 0:
        print(
            f"[silver_incremental_processing] quarantining "
            f"{orphaned_orders_count} orders with no matching customer"
        )

        merge_incremental(
            spark,
            source_df=orphaned_orders.withColumn(
                "_quarantine_reason",
                F.lit("missing customer"),
            ),
            target_table=f"{catalog}.quarantine.orders",
            merge_condition="target.order_id = source.order_id",
        )

        # Remove any rows that may have been written into Silver by an older
        # version of the pipeline before quarantine enforcement existed.
        remove_from_target(
            spark,
            keys_df=orphaned_orders.select("order_id"),
            target_table=f"{catalog}.silver.orders",
            delete_condition="target.order_id = source.order_id",
        )

    merge_incremental(
        spark,
        source_df=valid_orders,
        target_table=f"{catalog}.silver.orders",
        merge_condition="target.order_id = source.order_id",
    )

    print(
        f"[silver_incremental_processing] merged orders into "
        f"{catalog}.silver.orders"
    )

    # -----------------------------------------------------------------
    # Order items
    # -----------------------------------------------------------------
    order_items = spark.table(f"{catalog}.bronze.order_items")

    order_items = enforce_not_null(
        order_items,
        ["order_id", "order_item_id", "product_id"],
    )

    order_items = dedupe_latest(
        order_items,
        ["order_id", "order_item_id"],
        "_ingested_at",
    )

    # Bronze may contain string representations for numeric columns.
    order_items = (
        order_items.withColumn(
            "price",
            F.expr("try_cast(price AS double)"),
        )
        .withColumn(
            "freight_value",
            F.expr("try_cast(freight_value AS double)"),
        )
    )

    order_items = order_items.select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "price",
        "freight_value",
    )

    # An order item is valid only when both the order and product exist.
    valid_by_order, orphaned_by_order = split_by_referential_integrity(
        order_items,
        valid_orders,
        "order_id",
    )

    valid_order_items, orphaned_by_product = (
        split_by_referential_integrity(
            valid_by_order,
            products,
            "product_id",
        )
    )

    orphaned_order_items = (
        orphaned_by_order.withColumn(
            "_quarantine_reason",
            F.lit("missing order"),
        )
        .unionByName(
            orphaned_by_product.withColumn(
                "_quarantine_reason",
                F.lit("missing product"),
            )
        )
    )

    orphaned_order_items_count = orphaned_order_items.count()

    if orphaned_order_items_count > 0:
        print(
            f"[silver_incremental_processing] quarantining "
            f"{orphaned_order_items_count} order_items with no matching "
            f"order or product"
        )

        merge_incremental(
            spark,
            source_df=orphaned_order_items,
            target_table=f"{catalog}.quarantine.order_items",
            merge_condition=(
                "target.order_id = source.order_id "
                "AND target.order_item_id = source.order_item_id"
            ),
        )

        # Remove any orphaned rows written by an older pipeline version.
        remove_from_target(
            spark,
            keys_df=orphaned_order_items.select(
                "order_id",
                "order_item_id",
            ),
            target_table=f"{catalog}.silver.order_items",
            delete_condition=(
                "target.order_id = source.order_id "
                "AND target.order_item_id = source.order_item_id"
            ),
        )

    merge_incremental(
        spark,
        source_df=valid_order_items,
        target_table=f"{catalog}.silver.order_items",
        merge_condition=(
            "target.order_id = source.order_id "
            "AND target.order_item_id = source.order_item_id"
        ),
    )

    print(
        f"[silver_incremental_processing] merged order_items into "
        f"{catalog}.silver.order_items"
    )


if __name__ == "__main__":
    main()
