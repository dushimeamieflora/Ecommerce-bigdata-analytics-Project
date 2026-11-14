
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Inspect_Cohort_Data")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Load cleaned datasets
users = spark.read.parquet("data/processed/users")
transactions = spark.read.parquet("data/processed/transactions")

print("\n========== USERS DATASET ==========")
users.printSchema()
users.show(5, truncate=False)

print("\n========== TRANSACTIONS DATASET ==========")
transactions.printSchema()
transactions.show(5, truncate=False)

spark.stop()
