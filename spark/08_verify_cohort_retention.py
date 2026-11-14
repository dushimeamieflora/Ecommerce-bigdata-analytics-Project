
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    date_trunc,
    sum as spark_sum
)

spark = (
    SparkSession.builder
    .appName("Verify_Cohort_Retention")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

users = spark.read.parquet("data/processed/users")
transactions = spark.read.parquet("data/processed/transactions")

# Count all registered customers
print("\nTOTAL REGISTERED CUSTOMERS")
print(users.select(countDistinct("user_id")).first()[0])

# Count distinct customers who purchased each month
monthly_customers = (
    transactions
    .filter(col("status").isin("completed", "delivered", "shipped"))
    .withColumn("activity_month", date_trunc("month", col("timestamp")))
    .groupBy("activity_month")
    .agg(
        countDistinct("user_id").alias("active_customers"),
        spark_sum("total").alias("monthly_revenue")
    )
    .orderBy("activity_month")
)

print("\nMONTHLY PURCHASING CUSTOMERS")
monthly_customers.show(truncate=False)

spark.stop()
