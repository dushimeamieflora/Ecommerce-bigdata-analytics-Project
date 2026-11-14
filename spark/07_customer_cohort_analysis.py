
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    date_trunc,
    year,
    month,
    lit,
    sum as spark_sum,
    round as spark_round,
    when
)

# --------------------------------------------------
# 1. START SPARK
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Customer_Cohort_Analysis")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# --------------------------------------------------
# 2. LOAD CLEANED DATA
# --------------------------------------------------

users = spark.read.parquet("data/processed/users")
transactions = spark.read.parquet("data/processed/transactions")

# --------------------------------------------------
# 3. IDENTIFY CUSTOMER COHORTS
#    Cohort = month of customer registration
# --------------------------------------------------

user_cohorts = (
    users
    .filter(
        col("user_id").isNotNull() &
        col("registration_date").isNotNull()
    )
    .select(
        "user_id",
        "registration_date",
        date_trunc("month", col("registration_date")).alias("cohort_month")
    )
)

cohort_sizes = (
    user_cohorts
    .groupBy("cohort_month")
    .agg(
        countDistinct("user_id").alias("cohort_size")
    )
)

# --------------------------------------------------
# 4. SELECT VALID TRANSACTIONS
# --------------------------------------------------

valid_transactions = (
    transactions
    .filter(
        col("status").isin("completed", "delivered", "shipped") &
        col("user_id").isNotNull() &
        col("timestamp").isNotNull() &
        col("total").isNotNull()
    )
)

# --------------------------------------------------
# 5. CONNECT TRANSACTIONS TO CUSTOMER COHORTS
#    Exclude transactions before exact registration date
# --------------------------------------------------

cohort_activity = (
    valid_transactions.alias("t")
    .join(
        user_cohorts.alias("u"),
        col("t.user_id") == col("u.user_id"),
        "inner"
    )
    .filter(
        col("t.timestamp") >= col("u.registration_date")
    )
    .select(
        col("u.user_id").alias("user_id"),
        col("u.cohort_month").alias("cohort_month"),
        date_trunc("month", col("t.timestamp")).alias("activity_month"),
        col("t.transaction_id").alias("transaction_id"),
        col("t.total").alias("total")
    )
)

# --------------------------------------------------
# 6. CALCULATE MONTH NUMBER
# --------------------------------------------------

cohort_activity = cohort_activity.withColumn(
    "month_number",
    (
        (year(col("activity_month")) - year(col("cohort_month"))) * 12
        + month(col("activity_month"))
        - month(col("cohort_month"))
    )
)

# --------------------------------------------------
# 7. CALCULATE MONTHLY COHORT METRICS
# --------------------------------------------------

monthly_cohort = (
    cohort_activity
    .groupBy("cohort_month", "activity_month", "month_number")
    .agg(
        countDistinct("user_id").alias("active_customers"),
        countDistinct("transaction_id").alias("transaction_count"),
        spark_sum("total").alias("revenue")
    )
    .join(cohort_sizes, on="cohort_month", how="left")
    .withColumn(
        "retention_percentage",
        spark_round(
            col("active_customers") / col("cohort_size") * 100,
            2
        )
    )
    .withColumn(
        "average_spending",
        spark_round(
            col("revenue") / col("active_customers"),
            2
        )
    )
    .orderBy("cohort_month", "month_number")
)

# --------------------------------------------------
# 8. CREATE RETENTION MATRIX
# --------------------------------------------------

retention_matrix = (
    monthly_cohort
    .groupBy("cohort_month")
    .pivot("month_number")
    .agg(
        spark_round(
            spark_sum("active_customers") /
            spark_sum("cohort_size") * 100,
            2
        )
    )
    .orderBy("cohort_month")
)

# --------------------------------------------------
# 9. SAVE RESULTS
# --------------------------------------------------

monthly_cohort.write.mode("overwrite").parquet(
    "results/cohort_analysis/monthly_cohort"
)

retention_matrix.write.mode("overwrite").parquet(
    "results/cohort_analysis/retention_matrix"
)

# --------------------------------------------------
# 10. DISPLAY RESULTS
# --------------------------------------------------

print("\nCOHORT SIZES")
cohort_sizes.orderBy("cohort_month").show(50, truncate=False)

print("\nMONTHLY COHORT ANALYSIS")
monthly_cohort.show(50, truncate=False)

print("\nCUSTOMER RETENTION MATRIX (%)")
retention_matrix.show(50, truncate=False)

print("\nCOHORT ANALYSIS COMPLETED SUCCESSFULLY.")
print("Results saved in results/cohort_analysis/")

spark.stop()
