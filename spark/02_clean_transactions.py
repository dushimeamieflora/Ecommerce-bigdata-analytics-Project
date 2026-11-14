
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, to_timestamp, when, explode,
    count, sum as spark_sum
)

# Project paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
QUALITY_DIR = os.path.join(BASE_DIR, "results", "data_quality")

os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(QUALITY_DIR, exist_ok=True)

# Start Spark
spark = (
    SparkSession.builder
    .appName("CleanTransactions")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("Spark version:", spark.version)
print("Starting transaction cleaning...")

# Read transaction JSONL files
transactions = spark.read.json(
    os.path.join(RAW_DIR, "transactions_*.jsonl")
)

print("\nOriginal transaction count:", transactions.count())

# Standardize transaction fields
transactions = (
    transactions
    .withColumn("transaction_id", trim(col("transaction_id")))
    .withColumn("session_id", trim(col("session_id")))
    .withColumn("user_id", trim(col("user_id")))
    .withColumn("timestamp", to_timestamp(col("timestamp")))
    .withColumn("subtotal", col("subtotal").cast("double"))
    .withColumn("discount", col("discount").cast("double"))
    .withColumn("total", col("total").cast("double"))
    .withColumn("payment_method", trim(col("payment_method")))
    .withColumn("status", trim(col("status")))
)

# Clean nested transaction items
transactions = transactions.withColumn(
    "items",
    when(col("items").isNull(), None).otherwise(col("items"))
)

# Remove duplicates and invalid records
transactions = (
    transactions
    .dropDuplicates(["transaction_id"])
    .filter(col("transaction_id").isNotNull())
    .filter(col("user_id").isNotNull())
    .filter(col("timestamp").isNotNull())
    .filter(col("items").isNotNull())
    .filter(col("total").isNotNull())
    .filter(col("total") >= 0)
)

# Standardize missing text values
transactions = (
    transactions
    .withColumn(
        "payment_method",
        when(
            col("payment_method").isNull() |
            (col("payment_method") == ""),
            "Unknown"
        ).otherwise(col("payment_method"))
    )
    .withColumn(
        "status",
        when(
            col("status").isNull() |
            (col("status") == ""),
            "Unknown"
        ).otherwise(col("status"))
    )
)

# Assess cleaned data
total_records = transactions.count()
duplicate_ids = (
    transactions.groupBy("transaction_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

missing_values = {}

for column_name in transactions.columns:
    missing_values[column_name] = transactions.filter(
        col(column_name).isNull()
    ).count()

print("\n--- TRANSACTION DATA QUALITY ---")
print("Clean transaction count:", total_records)
print("Duplicate transaction IDs:", duplicate_ids)
print("Missing values:", missing_values)

# Save cleaned transactions
output_path = os.path.join(PROCESSED_DIR, "transactions")
transactions.write.mode("overwrite").parquet(output_path)

# Save quality report
quality_report = {
    "dataset": "transactions",
    "clean_records": total_records,
    "duplicate_transaction_ids": duplicate_ids,
    "missing_values": missing_values
}

report_path = os.path.join(
    QUALITY_DIR, "transactions_quality.json"
)

with open(report_path, "w", encoding="utf-8") as file:
    json.dump(quality_report, file, indent=4)

print("\nCLEANED TRANSACTIONS")
transactions.select(
    "transaction_id",
    "user_id",
    "timestamp",
    "subtotal",
    "discount",
    "total",
    "payment_method",
    "status"
).show(5, truncate=False)

print("\nCleaned transaction data saved to:", output_path)
print("Quality report saved to:", report_path)
print("\nTRANSACTION CLEANING COMPLETED SUCCESSFULLY")

spark.stop()
