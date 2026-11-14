
"""
Project: E-commerce Big Data Analytics
Part 2: Apache Spark Batch Processing

Script: 01_clean_normalize.py

Purpose:
1. Load users, products and categories from raw JSON files.
2. Inspect and assess data quality.
3. Remove duplicate records using primary identifiers.
4. Standardize column names and data types.
5. Handle missing values appropriately.
6. Save cleaned datasets in Parquet format.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    to_timestamp,
    when,
    count,
    sum as spark_sum
)
from pyspark.sql.types import DoubleType, IntegerType
import os
import json


# --------------------------------------------------
# 1. PROJECT PATHS
# --------------------------------------------------

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
QUALITY_DIR = os.path.join(BASE_DIR, "results", "data_quality")

os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(QUALITY_DIR, exist_ok=True)


# --------------------------------------------------
# 2. CREATE SPARK SESSION
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("EcommerceDataCleaning")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\nSpark version:", spark.version)
print("Starting data cleaning process...")


# --------------------------------------------------
# 3. DATA QUALITY ASSESSMENT FUNCTION
# --------------------------------------------------

quality_report = {}


def assess_quality(df, dataset_name, id_column):

    total_records = df.count()

    duplicate_records = (
        total_records - df.dropDuplicates([id_column]).count()
    )

    missing_values = {}

    for column_name in df.columns:

        missing_count = df.filter(
            col(column_name).isNull()
        ).count()

        if missing_count > 0:
            missing_values[column_name] = missing_count

    quality_report[dataset_name] = {
        "total_records": total_records,
        "duplicate_records": duplicate_records,
        "missing_values": missing_values
    }

    print(f"\n--- {dataset_name.upper()} DATA QUALITY ---")
    print("Total records:", total_records)
    print("Duplicate records:", duplicate_records)
    print("Missing values:", missing_values)


# --------------------------------------------------
# 4. CLEAN USERS DATA
# --------------------------------------------------

print("\nLoading users...")

users = spark.read.option("multiline", "true").json(
    os.path.join(RAW_DIR, "users.json")
)

users = users.withColumn(
    "registration_date",
    to_timestamp(col("registration_date"))
).withColumn(
    "last_active",
    to_timestamp(col("last_active"))
).withColumn(
    "age",
    col("age").cast(IntegerType())
).withColumn(
    "gender",
    trim(col("gender"))
)

assess_quality(users, "users", "user_id")

users_clean = (
    users
    .dropDuplicates(["user_id"])
    .filter(col("user_id").isNotNull())
    .withColumn(
        "gender",
        when(
            col("gender").isNull() | (col("gender") == ""),
            "Unknown"
        ).otherwise(col("gender"))
    )
)

users_clean.write.mode("overwrite").parquet(
    os.path.join(PROCESSED_DIR, "users")
)

print("Clean users:", users_clean.count())


# --------------------------------------------------
# 5. CLEAN PRODUCTS DATA
# --------------------------------------------------

print("\nLoading products...")

products = spark.read.option("multiline", "true").json(
    os.path.join(RAW_DIR, "products.json")
)

products = (
    products
    .withColumn("name", trim(col("name")))
    .withColumn("base_price", col("base_price").cast(DoubleType()))
    .withColumn("current_stock", col("current_stock").cast(IntegerType()))
    .withColumn("is_active", col("is_active").cast("boolean"))
    .withColumn("creation_date", to_timestamp(col("creation_date")))
)

assess_quality(products, "products", "product_id")

products_clean = (
    products
    .dropDuplicates(["product_id"])
    .filter(col("product_id").isNotNull())
    .filter(col("base_price").isNotNull())
    .filter(col("base_price") >= 0)
    .withColumn(
        "name",
        when(
            col("name").isNull() | (col("name") == ""),
            "Unknown Product"
        ).otherwise(col("name"))
    )
)

products_clean.write.mode("overwrite").parquet(
    os.path.join(PROCESSED_DIR, "products")
)

print("Clean products:", products_clean.count())


# --------------------------------------------------
# 6. CLEAN CATEGORIES DATA
# --------------------------------------------------

print("\nLoading categories...")

categories = spark.read.option("multiline", "true").json(
    os.path.join(RAW_DIR, "categories.json")
)

categories = categories.withColumn(
    "name",
    trim(col("name"))
)

assess_quality(categories, "categories", "category_id")

categories_clean = (
    categories
    .dropDuplicates(["category_id"])
    .filter(col("category_id").isNotNull())
    .withColumn(
        "name",
        when(
            col("name").isNull() | (col("name") == ""),
            "Unknown Category"
        ).otherwise(col("name"))
    )
)

categories_clean.write.mode("overwrite").parquet(
    os.path.join(PROCESSED_DIR, "categories")
)

print("Clean categories:", categories_clean.count())


# --------------------------------------------------
# 7. SAVE DATA QUALITY REPORT
# --------------------------------------------------

report_path = os.path.join(
    QUALITY_DIR,
    "initial_data_quality.json"
)

with open(report_path, "w", encoding="utf-8") as file:

    json.dump(
        quality_report,
        file,
        indent=4
    )

print("\nData quality report saved:", report_path)


# --------------------------------------------------
# 8. DISPLAY CLEANED DATA
# --------------------------------------------------

print("\nCLEANED USERS")
users_clean.show(5, truncate=False)

print("\nCLEANED PRODUCTS")
products_clean.show(5, truncate=False)

print("\nCLEANED CATEGORIES")
categories_clean.show(5, truncate=False)


# --------------------------------------------------
# 9. STOP SPARK
# --------------------------------------------------

spark.stop()

print("\n===================================")
print("DATA CLEANING COMPLETED SUCCESSFULLY")
print("===================================")
