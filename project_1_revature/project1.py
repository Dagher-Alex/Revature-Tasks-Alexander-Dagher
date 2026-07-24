"""
Project 1 — End-to-End Retail Data Pipeline
=============================================
Reads customers.csv, orders.csv, products.csv from S3,
cleans and transforms every dataset, then writes Apache Iceberg
tables via the AWS Glue catalog so Snowflake can query them.

To run on EMR (SSH into primary node first):
    spark-submit project1.py
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, DoubleType, DateType, BooleanType
)

# ── Spark Session ─────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("Project_1")
    # Enable Iceberg SQL extensions
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    # Register a catalog named "glue_catalog"
    .config(
        "spark.sql.catalog.glue_catalog",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    # Use AWS Glue as the metadata store
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog"
    )
    # S3 location where Iceberg table data and metadata will be stored
    .config(
        "spark.sql.catalog.glue_catalog.warehouse",
        "s3://rev1-249954438267-us-east-1-an/iceberg/"
    )
    # Use S3FileIO for reading/writing data in Amazon S3
    .config(
        "spark.sql.catalog.glue_catalog.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    )
    .getOrCreate()
)

S3_BUCKET = "s3://rev1-249954438267-us-east-1-an"

customers_schema = StructType([
    StructField("customer_id",    StringType()),
    StructField("first_name",     StringType()),
    StructField("last_name",      StringType()),
    StructField("email",          StringType()),
    StructField("phone",          StringType()),
    StructField("signup_date",    StringType()),   # multiple date formats
    StructField("country",        StringType()),
    StructField("state",          StringType()),
    StructField("postal_code",    StringType()),
    StructField("is_active",      StringType()),   # TRUE/Yes/Y/1/false/No/N/0
    StructField("loyalty_points", StringType()),   # may contain NULL or text
])

products_schema = StructType([
    StructField("product_id",     StringType()),
    StructField("product_name",   StringType()),
    StructField("category",       StringType()),
    StructField("brand",          StringType()),
    StructField("price",          StringType()),   # may contain $ or NULL
    StructField("cost",           StringType()),
    StructField("stock_quantity", StringType()),
    StructField("weight_kg",      StringType()),   # may contain "?"
    StructField("created_date",   StringType()),
    StructField("is_active",      StringType()),
])

orders_schema = StructType([
    StructField("order_id",       StringType()),
    StructField("customer_id",    StringType()),
    StructField("product_id",     StringType()),
    StructField("order_date",     StringType()),
    StructField("ship_date",      StringType()),
    StructField("quantity",       StringType()),
    StructField("unit_price",     StringType()),   # may contain $
    StructField("discount_pct",   StringType()),   # may contain text or >100
    StructField("total_amount",   StringType()),
    StructField("payment_method", StringType()),
    StructField("order_status",   StringType()),
])

# ── Read CSVs from S3 ─────────────────────────────────────────────────────────

customers_df = (
    spark.read
    .option("header", True)
    .schema(customers_schema)
    .csv(f"{S3_BUCKET}/customers.csv")
)

products_df = (
    spark.read
    .option("header", True)
    .schema(products_schema)
    .csv(f"{S3_BUCKET}/products.csv")
)

orders_df = (
    spark.read
    .option("header", True)
    .schema(orders_schema)
    .csv(f"{S3_BUCKET}/orders.csv")
)

# Create the Iceberg database (namespace) in AWS Glue if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.iceberg_catalog_db")

# ── Shared Helper Functions ───────────────────────────────────────────────────

# Text values that represent "missing" data — treated as null
NULL_STRINGS = ["NULL", "null", "N/A", "n/a", "None", "none", "NaN", "nan", "na", ""]


def clean_string_cols(df):
    """
    Trim leading/trailing whitespace from every string column.
    Replace blank values and null-like strings ("NULL", "N/A", etc.) with null.
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            trimmed = F.trim(F.col(field.name))
            df = df.withColumn(
                field.name,
                F.when(trimmed.isin(NULL_STRINGS), None).otherwise(trimmed)
            )
    return df


def parse_date(col_expr):
    """
    Try common date formats in order; return the first that parses successfully.
    Unparseable values become null.
    Note: MM-dd-yyyy is tried before dd-MM-yyyy because the source data is
    US-based. If your data uses European order, swap those two lines.
    """
    return F.coalesce(
        F.to_date(col_expr, "yyyy-MM-dd"),   # 2023-07-01
        F.to_date(col_expr, "yyyy/MM/dd"),   # 2023/07/01
        F.to_date(col_expr, "MM-dd-yyyy"),   # 07-01-2023
        F.to_date(col_expr, "dd-MM-yyyy"),   # 01-07-2023
        F.to_date(col_expr, "MM/dd/yyyy"),   # 07/01/2023
    )


def parse_boolean(col_expr):
    """
    Normalize various boolean representations to a proper BooleanType.
    Unrecognized values become null.
    """
    upper = F.upper(col_expr)
    return (
        F.when(upper.isin("TRUE", "YES", "Y", "1"), F.lit(True))
         .when(upper.isin("FALSE", "NO", "N", "0"), F.lit(False))
         .otherwise(None)
    ).cast(BooleanType())


def strip_currency(col_expr):
    """
    Remove $ signs and thousands-separator commas from a numeric string.
    e.g. "$1,234.99" -> "1234.99"
    """
    no_dollar = F.regexp_replace(col_expr, r"\$", "")
    # Remove commas used as thousands separators (comma followed by 3 digits)
    return F.regexp_replace(no_dollar, r",(?=\d{3})", "")


def fix_european_decimal(col_expr):
    """
    Replace a decimal comma with a period for European-format numbers.
    e.g. "399,99" -> "399.99"
    Matches: digits, comma, exactly 2 digits at end of string.
    """
    return F.regexp_replace(col_expr, r"^(\d+),(\d{2})$", "$1.$2")


print("\n" + "="*60)
print("CUSTOMERS CLEANING")
print("="*60)
print(f"Raw row count: {customers_df.count()}")

c = clean_string_cols(customers_df)

# 1. Remove exact duplicate rows, keeping first occurrence
c = c.dropDuplicates(["customer_id"])

# 2. Cast customer_id to integer — "XYZ" becomes null and is dropped later
c = c.withColumn("customer_id", F.col("customer_id").cast(IntegerType()))

# 3. Validate and normalize email addresses
#    Invalid emails (missing @, double @@, no TLD) become null
email_pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
c = c.withColumn(
    "email",
    F.when(
        F.col("email").rlike(email_pattern),
        F.lower(F.col("email"))
    )
    # emails that don't match the pattern become null (implicit otherwise)
)

# 4. Clean phone numbers
#    Strip all non-digit characters, then keep only 10–15 digit results
c = c.withColumn("phone", F.regexp_replace(F.col("phone"), r"\D", ""))
c = c.withColumn(
    "phone",
    F.when(F.length(F.col("phone")).between(10, 15), F.col("phone"))
)

# 5. Parse signup_date — handles multiple date formats; impossible dates become null
c = c.withColumn("signup_date", parse_date(F.col("signup_date")))

# 6. Standardize country name — normalize common variations to "USA"
c = c.withColumn(
    "country",
    F.when(
        F.col("country").isin("US", "U.S.A.", "United States"),
        F.lit("USA")
    ).otherwise(F.col("country"))
)

# 7. Standardize state to uppercase (e.g. "mn" → "MN")
c = c.withColumn("state", F.upper(F.col("state")))

# 8. Normalize is_active to Boolean (handles Yes/No/Y/N/1/0/true/false/maybe)
c = c.withColumn("is_active", parse_boolean(F.col("is_active")))

# 9. Cast loyalty_points to integer; negative values → 0
c = c.withColumn("loyalty_points", F.col("loyalty_points").cast(IntegerType()))
c = c.withColumn(
    "loyalty_points",
    F.when(F.col("loyalty_points") < 0, F.lit(0)).otherwise(F.col("loyalty_points"))
)

# 10. Drop rows missing any required field
c = c.dropna(
    subset=["customer_id", "first_name", "last_name", "email", "signup_date"]
)

print(f"Clean row count: {c.count()}")

# Write to Iceberg
(
    c.writeTo("glue_catalog.iceberg_catalog_db.customers")
     .using("iceberg")
     .createOrReplace()
)

spark.sql("""
    SELECT * FROM glue_catalog.iceberg_catalog_db.customers LIMIT 10
""").show(truncate=False)

print("\n" + "="*60)
print("PRODUCTS CLEANING")
print("="*60)
print(f"Raw row count: {products_df.count()}")

p = clean_string_cols(products_df)

# 1. Remove duplicate product_id rows
p = p.dropDuplicates(["product_id"])

# 2. Validate product_id format: must be "P" followed by one or more digits
#    Rows like "BADID" have their product_id set to null and are dropped later
p = p.withColumn(
    "product_id",
    F.when(F.col("product_id").rlike(r"^P\d+$"), F.col("product_id"))
)

# 3. Clean product_name: remove stray quotation marks, standardize capitalization
p = p.withColumn("product_name", F.regexp_replace(F.col("product_name"), r'"+', ""))
p = p.withColumn("product_name", F.initcap(F.col("product_name")))

# 4. Standardize category and brand to title case
p = p.withColumn("category", F.initcap(F.col("category")))
p = p.withColumn("brand",    F.initcap(F.col("brand")))

# 5. Clean price:
#    a) Remove $ signs and thousands-separator commas
#    b) Fix European decimal comma ("399,99" → "399.99")
#    c) Cast to Double
#    d) Negative price → null
p = p.withColumn("price", strip_currency(F.col("price")))
p = p.withColumn("price", fix_european_decimal(F.col("price")))
p = p.withColumn("price", F.col("price").cast(DoubleType()))
p = p.withColumn(
    "price",
    F.when(F.col("price") < 0, None).otherwise(F.col("price"))
)

# 6. Clean cost (same approach as price)
p = p.withColumn("cost", strip_currency(F.col("cost")))
p = p.withColumn("cost", F.col("cost").cast(DoubleType()))
p = p.withColumn(
    "cost",
    F.when(F.col("cost") < 0, None).otherwise(F.col("cost"))
)

# 7. stock_quantity: cast to integer; negative values → 0
p = p.withColumn("stock_quantity", F.col("stock_quantity").cast(IntegerType()))
p = p.withColumn(
    "stock_quantity",
    F.when(F.col("stock_quantity") < 0, F.lit(0)).otherwise(F.col("stock_quantity"))
)

# 8. weight_kg: strip non-numeric characters (handles "?"), cast to float
#    Negative weight → null
p = p.withColumn("weight_kg", F.regexp_replace(F.col("weight_kg"), r"[^\d.]", ""))
p = p.withColumn("weight_kg", F.col("weight_kg").cast(FloatType()))
p = p.withColumn(
    "weight_kg",
    F.when(F.col("weight_kg") < 0, None).otherwise(F.col("weight_kg"))
)

# 9. Parse created_date — handles multiple date formats
p = p.withColumn("created_date", parse_date(F.col("created_date")))

# 10. Normalize is_active to Boolean
p = p.withColumn("is_active", parse_boolean(F.col("is_active")))

# 11. Drop rows missing required fields
p = p.dropna(subset=["product_id", "product_name", "category", "price"])

print(f"Clean row count: {p.count()}")

# Write to Iceberg
(
    p.writeTo("glue_catalog.iceberg_catalog_db.products")
     .using("iceberg")
     .createOrReplace()
)

spark.sql("""
    SELECT * FROM glue_catalog.iceberg_catalog_db.products LIMIT 10
""").show(truncate=False)


print("\n" + "="*60)
print("ORDERS CLEANING")
print("="*60)
print(f"Raw row count: {orders_df.count()}")

o = clean_string_cols(orders_df)

# 1. Remove duplicate order_id rows
o = o.dropDuplicates(["order_id"])

# 2. Parse order_date and ship_date — handles multiple formats
o = o.withColumn("order_date", parse_date(F.col("order_date")))
o = o.withColumn("ship_date",  parse_date(F.col("ship_date")))

# 3. Ensure ship_date is not before order_date — set to null if it is
o = o.withColumn(
    "ship_date",
    F.when(
        F.col("ship_date") >= F.col("order_date"),
        F.col("ship_date")
    ).otherwise(None)
)

# 4. quantity: cast to integer, must be a positive whole number
o = o.withColumn("quantity", F.col("quantity").cast(IntegerType()))
o = o.withColumn(
    "quantity",
    F.when(F.col("quantity") > 0, F.col("quantity")).otherwise(None)
)

# 5. unit_price: strip $, cast to Double, must be >= 0
o = o.withColumn("unit_price", strip_currency(F.col("unit_price")))
o = o.withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
o = o.withColumn(
    "unit_price",
    F.when(F.col("unit_price") >= 0, F.col("unit_price")).otherwise(None)
)

# 6. discount_pct: non-numeric values (e.g. "abc") → null after cast;
#    must be between 0 and 100 (values stored as percentage, not decimal)
o = o.withColumn("discount_pct", F.col("discount_pct").cast(FloatType()))
o = o.withColumn(
    "discount_pct",
    F.when(
        F.col("discount_pct").between(0, 100),
        F.col("discount_pct")
    ).otherwise(None)
)

# 7. total_amount: strip $, cast to Double, must be >= 0
o = o.withColumn("total_amount", strip_currency(F.col("total_amount")))
o = o.withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
o = o.withColumn(
    "total_amount",
    F.when(F.col("total_amount") >= 0, F.col("total_amount")).otherwise(None)
)

# 8. Standardize order_status to uppercase; reject unrecognized values
o = o.withColumn("order_status", F.upper(F.col("order_status")))
valid_statuses = ["PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
o = o.withColumn(
    "order_status",
    F.when(F.col("order_status").isin(valid_statuses), F.col("order_status"))
)

# 9. Standardize payment_method to title case (e.g. "VISA" → "Visa")
o = o.withColumn("payment_method", F.initcap(F.col("payment_method")))

# 10. Validate foreign keys — keep only orders whose customer_id
#     exists in the cleaned customers table
valid_customer_ids = c.select(
    F.col("customer_id").cast(StringType()).alias("customer_id")
)
o = o.join(valid_customer_ids, on="customer_id", how="inner")

# 11. Validate foreign keys — keep only orders whose product_id
#     exists in the cleaned products table
valid_product_ids = p.select("product_id")
o = o.join(valid_product_ids, on="product_id", how="inner")

# 12. Drop rows missing required fields
o = o.dropna(
    subset=["order_id", "customer_id", "product_id", "order_date", "quantity"]
)

print(f"Clean row count: {o.count()}")

# Write to Iceberg
(
    o.writeTo("glue_catalog.iceberg_catalog_db.orders")
     .using("iceberg")
     .createOrReplace()
)

spark.sql("""
    SELECT * FROM glue_catalog.iceberg_catalog_db.orders LIMIT 10
""").show(truncate=False)


# ── Record Count Validation ───────────────────────────────────────────────────

print("\n" + "="*60)
print("RECORD COUNT VALIDATION")
print("="*60)
print(f"Customers : raw={customers_df.count():>4}  →  clean={c.count():>4}")
print(f"Products  : raw={products_df.count():>4}  →  clean={p.count():>4}")
print(f"Orders    : raw={orders_df.count():>4}  →  clean={o.count():>4}")

spark.stop()
