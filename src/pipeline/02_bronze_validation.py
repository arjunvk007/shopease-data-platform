from pyspark.sql import SparkSession

from src.pipeline.runtime_config import get_runtime_config

spark = SparkSession.builder.getOrCreate()

CATALOG, ENVIRONMENT = get_runtime_config()


BRONZE_TABLES = [
    "customers",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "geolocation",
    "category_translation",
]


REQUIRED_METADATA_COLUMNS = {
    "_ingested_at",
    "_source_file",
}


print("=" * 70)
print("ShopEase Bronze Validation")
print(f"Environment : {ENVIRONMENT}")
print(f"Catalog     : {CATALOG}")
print("=" * 70)


validation_errors = []


for dataset in BRONZE_TABLES:
    table_name = f"{CATALOG}.bronze.{dataset}"

    print(f"\nValidating: {table_name}")

    # ---------------------------------------------------------------
    # 1. Verify table exists
    # ---------------------------------------------------------------
    if not spark.catalog.tableExists(table_name):
        error = f"{table_name}: table does not exist"
        validation_errors.append(error)
        print(f"FAILED: {error}")
        continue

    print("PASS: table exists")

    # ---------------------------------------------------------------
    # 2. Read table
    # ---------------------------------------------------------------
    df = spark.table(table_name)

    print("PASS: table is readable")

    # ---------------------------------------------------------------
    # 3. Verify table contains records
    # ---------------------------------------------------------------
    row_count = df.count()

    if row_count <= 0:
        error = f"{table_name}: table contains zero records"
        validation_errors.append(error)
        print(f"FAILED: {error}")
    else:
        print(f"PASS: row count = {row_count}")

    # ---------------------------------------------------------------
    # 4. Verify ingestion metadata columns
    # ---------------------------------------------------------------
    actual_columns = set(df.columns)

    missing_metadata_columns = REQUIRED_METADATA_COLUMNS - actual_columns

    if missing_metadata_columns:
        error = (
            f"{table_name}: missing ingestion metadata columns: "
            f"{sorted(missing_metadata_columns)}"
        )
        validation_errors.append(error)
        print(f"FAILED: {error}")
    else:
        print(
            "PASS: ingestion metadata columns present "
            "(_ingested_at, _source_file)"
        )


# -------------------------------------------------------------------
# Final quality gate
# -------------------------------------------------------------------

print("\n" + "=" * 70)

if validation_errors:
    print("BRONZE VALIDATION FAILED")
    print("=" * 70)

    for error in validation_errors:
        print(f"- {error}")

    raise RuntimeError(
        f"Bronze validation failed with "
        f"{len(validation_errors)} error(s)."
    )


print("BRONZE VALIDATION PASSED")
print(
    f"Validated {len(BRONZE_TABLES)} Bronze tables "
    f"in {CATALOG}.bronze"
)
print("=" * 70)