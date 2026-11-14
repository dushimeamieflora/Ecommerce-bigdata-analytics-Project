
from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, count

spark = (
    SparkSession.builder
    .appName("Inspect_Cohort_Transactions")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

transactions = spark.read.parquet(
    "data/processed/transactions"
)

print("\n========== TRANSACTION DATE RANGE ==========")

transactions.select(
    min("timestamp").alias("earliest_transaction"),
    max("timestamp").alias("latest_transaction")
).show(truncate=False)

print("\n========== TRANSACTION STATUS DISTRIBUTION ==========")

transactions.groupBy("status").agg(
    count("*").alias("transaction_count")
).orderBy("transaction_count", ascending=False).show()

print("\n========== TRANSACTION TOTALS ==========")

transactions.select(
    count("*").alias("total_transactions")
).show()

spark.stop()
