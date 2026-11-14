
from pymongo import MongoClient

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    countDistinct,
    sum as spark_sum
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    ArrayType
)

# ==================================================
# 1. START SPARK
# ==================================================

spark = (
    SparkSession.builder
    .appName("MongoDB_to_Spark_Integration")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ==================================================
# 2. CONNECT TO MONGODB
# ==================================================

client = MongoClient("mongodb://localhost:27017")

db = client["ecommerce_analytics"]

collection = db["transactions"]

print("\nMONGODB CONNECTION SUCCESSFUL")

# ==================================================
# 3. DEFINE MONGODB TRANSACTION SCHEMA
# ==================================================

item_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("quantity", LongType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("subtotal", DoubleType(), True)
])

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("items", ArrayType(item_schema), True),
    StructField("subtotal", DoubleType(), True),
    StructField("discount", DoubleType(), True),
    StructField("total", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("status", StringType(), True)
])

# ==================================================
# 4. RETRIEVE A SAMPLE OF TRANSACTIONS
# ==================================================

projection = {
    "_id": 0,
    "transaction_id": 1,
    "session_id": 1,
    "user_id": 1,
    "timestamp": 1,
    "items": 1,
    "subtotal": 1,
    "discount": 1,
    "total": 1,
    "payment_method": 1,
    "status": 1
}

documents = list(
    collection.find({}, projection).limit(10000)
)

print("\nMONGODB DATA RETRIEVED")
print("Sample transactions:", len(documents))

client.close()

# ==================================================
# 5. CREATE SPARK DATAFRAME
# ==================================================

mongo_transactions = spark.createDataFrame(
    documents,
    schema=transaction_schema
)

# Convert timestamp string into Spark timestamp
mongo_transactions = (
    mongo_transactions
    .withColumn(
        "timestamp",
        to_timestamp(col("timestamp"))
    )
)

# ==================================================
# 6. REGISTER SPARK SQL VIEW
# ==================================================

mongo_transactions.createOrReplaceTempView(
    "mongo_transactions"
)

print("\nMONGODB DATAFRAME SCHEMA")
mongo_transactions.printSchema()

print("\nSAMPLE MONGODB TRANSACTIONS")
mongo_transactions.show(5, truncate=False)

# ==================================================
# 7. PERFORM SPARK SQL ANALYSIS
# ==================================================

transaction_summary = spark.sql("""
    SELECT
        status,
        COUNT(DISTINCT transaction_id) AS total_transactions,
        COUNT(DISTINCT user_id) AS unique_customers,
        ROUND(SUM(total), 2) AS total_revenue
    FROM mongo_transactions
    GROUP BY status
    ORDER BY total_revenue DESC
""")

print("\nTRANSACTION SUMMARY FROM MONGODB")
transaction_summary.show(truncate=False)

# ==================================================
# 8. CALCULATE OVERALL SUMMARY
# ==================================================

print("\nOVERALL MONGODB SAMPLE SUMMARY")

mongo_transactions.select(
    countDistinct("transaction_id").alias("transactions"),
    countDistinct("user_id").alias("customers"),
    spark_sum("total").alias("revenue")
).show(truncate=False)

# ==================================================
# 9. SAVE SPARK DATAFRAME
# ==================================================

mongo_transactions.write.mode("overwrite").parquet(
    "results/integration/mongodb_transactions_sample"
)

transaction_summary.write.mode("overwrite").parquet(
    "results/integration/mongodb_transaction_summary"
)

# ==================================================
# 10. COMPLETE
# ==================================================

print("\nMONGODB TO SPARK INTEGRATION COMPLETED!")
print("Results saved in results/integration/")

spark.stop()
