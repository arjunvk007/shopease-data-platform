"""
Pipeline entry point: Silver-layer validation.

Validates the core Silver tables after incremental processing and blocks
downstream Gold processing when critical data-quality or referential-
integrity checks fail.
"""

import inspect
import os
import sys

from pyspark.sql import SparkSession

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

spark = SparkSession.builder.getOrCreate()

CATALOG, ENVIRONMENT = get_runtime_config()


# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

SILVER_TABLES = [
    "customers",
    "products",
    "orders",
    "order_items",
]


print("=" * 70)
print("ShopEase Silver Validation")
print(f"Environment : {ENVIRONMENT}")
print(f"Catalog     : {CATALOG}")
print("=" * 70)


validation_errors = []


# -------------------------------------------------------------------
# 1. Table existence and row-count validation
# -------------------------------------------------------------------

for dataset in SILVER_TABLES:
    table_name = f"{CATALOG}.silver.{dataset}"

    print(f"\nValidating Silver table: {table_name}")

    if not spark.catalog.tableExists(table_name):
        error = f"{table_name}: table does not exist"
        validation_errors.append(error)
        print(f"FAILED: {error}")
        continue

    print("PASS: table exists")

    df = spark.table(table_name)

    row_count = df.count()

    if row_count <= 0:
        error = f"{table_name}: table contains zero records"
        validation_errors.append(error)
        print(f"FAILED: {error}")
    else:
        print(f"PASS: row count = {row_count}")


# -------------------------------------------------------------------
# 2. Load Silver tables
# -------------------------------------------------------------------

customers_table = f"{CATALOG}.silver.customers"
products_table = f"{CATALOG}.silver.products"
orders_table = f"{CATALOG}.silver.orders"
order_items_table = f"{CATALOG}.silver.order_items"


required_tables = [
    customers_table,
    products_table,
    orders_table,
    order_items_table,
]


tables_available = all(
    spark.catalog.tableExists(table_name)
    for table_name in required_tables
)


if tables_available:
    customers = spark.table(customers_table)
    products = spark.table(products_table)
    orders = spark.table(orders_table)
    order_items = spark.table(order_items_table)

    # ---------------------------------------------------------------
    # 3. Referential integrity:
    #    orders.customer_id -> customers.customer_id
    # ---------------------------------------------------------------

    orphaned_orders = (
        orders.alias("orders")
        .join(
            customers.select("customer_id").alias("customers"),
            on="customer_id",
            how="left_anti",
        )
    )

    orphaned_orders_count = orphaned_orders.count()

    if orphaned_orders_count > 0:
        error = (
            f"orders -> customers: found "
            f"{orphaned_orders_count} orphaned record(s) "
            f"on customer_id"
        )

        validation_errors.append(error)
        print(f"FAILED: {error}")
    else:
        print(
            "PASS: orders.customer_id -> "
            "customers.customer_id referential integrity"
        )

    # ---------------------------------------------------------------
    # 4. Referential integrity:
    #    order_items.order_id -> orders.order_id
    # ---------------------------------------------------------------

    orphaned_order_items_by_order = (
        order_items.alias("order_items")
        .join(
            orders.select("order_id").alias("orders"),
            on="order_id",
            how="left_anti",
        )
    )

    orphaned_order_items_by_order_count = (
        orphaned_order_items_by_order.count()
    )

    if orphaned_order_items_by_order_count > 0:
        error = (
            f"order_items -> orders: found "
            f"{orphaned_order_items_by_order_count} "
            f"orphaned record(s) on order_id"
        )

        validation_errors.append(error)
        print(f"FAILED: {error}")
    else:
        print(
            "PASS: order_items.order_id -> "
            "orders.order_id referential integrity"
        )

    # ---------------------------------------------------------------
    # 5. Referential integrity:
    #    order_items.product_id -> products.product_id
    # ---------------------------------------------------------------

    orphaned_order_items_by_product = (
        order_items.alias("order_items")
        .join(
            products.select("product_id").alias("products"),
            on="product_id",
            how="left_anti",
        )
    )

    orphaned_order_items_by_product_count = (
        orphaned_order_items_by_product.count()
    )

    if orphaned_order_items_by_product_count > 0:
        error = (
            f"order_items -> products: found "
            f"{orphaned_order_items_by_product_count} "
            f"orphaned record(s) on product_id"
        )

        validation_errors.append(error)
        print(f"FAILED: {error}")
    else:
        print(
            "PASS: order_items.product_id -> "
            "products.product_id referential integrity"
        )


# -------------------------------------------------------------------
# 6. Final quality gate
# -------------------------------------------------------------------

print("\n" + "=" * 70)

if validation_errors:
    print("SILVER VALIDATION FAILED")
    print("=" * 70)

    for error in validation_errors:
        print(f"- {error}")

    raise RuntimeError(
        f"Silver validation failed with "
        f"{len(validation_errors)} error(s)."
    )


print("SILVER VALIDATION PASSED")
print(f"Environment : {ENVIRONMENT}")
print(f"Catalog     : {CATALOG}")
print("=" * 70)
