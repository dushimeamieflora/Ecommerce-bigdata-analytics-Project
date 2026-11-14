
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    count,
    round as spark_round
)

# --------------------------------------------------
# 1. Start Spark
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Ecommerce_Product_Recommendations")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Spark session started.")

# --------------------------------------------------
# 2. Load cleaned datasets
# --------------------------------------------------

transactions = spark.read.parquet(
    "data/processed/transactions"
)

products = spark.read.parquet(
    "data/processed/products"
)

print("Cleaned datasets loaded.")

# --------------------------------------------------
# 3. Extract products from each transaction
# --------------------------------------------------

transaction_products = (
    transactions
    .select(
        "transaction_id",
        explode("items").alias("item")
    )
    .select(
        "transaction_id",
        col("item.product_id").alias("product_id")
    )
    .filter(col("product_id").isNotNull())
    .dropDuplicates(["transaction_id", "product_id"])
)

print("Transaction products extracted.")

# --------------------------------------------------
# 4. Generate product pairs
# --------------------------------------------------

first = transaction_products.alias("first")
second = transaction_products.alias("second")

product_pairs = (
    first.join(
        second,
        (col("first.transaction_id") ==
         col("second.transaction_id")) &
        (col("first.product_id") <
         col("second.product_id"))
    )
    .select(
        col("first.product_id").alias("product_a"),
        col("second.product_id").alias("product_b"),
        col("first.transaction_id").alias("transaction_id")
    )
)

# --------------------------------------------------
# 5. Count how often products are purchased together
# --------------------------------------------------

recommendations = (
    product_pairs
    .groupBy("product_a", "product_b")
    .agg(
        count("transaction_id").alias("co_purchase_count")
    )
    .filter(col("co_purchase_count") >= 2)
)

# --------------------------------------------------
# 6. Calculate support
# --------------------------------------------------

total_transactions = transactions.count()

recommendations = recommendations.withColumn(
    "support_percentage",
    spark_round(
        col("co_purchase_count") * 100 / total_transactions,
        4
    )
)

# --------------------------------------------------
# 7. Add product names
# --------------------------------------------------

product_names_a = products.select(
    col("product_id").alias("product_a"),
    col("name").alias("product_a_name")
)

product_names_b = products.select(
    col("product_id").alias("product_b"),
    col("name").alias("product_b_name")
)

recommendations = (
    recommendations
    .join(product_names_a, on="product_a", how="left")
    .join(product_names_b, on="product_b", how="left")
    .select(
        "product_a",
        "product_a_name",
        "product_b",
        "product_b_name",
        "co_purchase_count",
        "support_percentage"
    )
    .orderBy(col("co_purchase_count").desc())
)

# --------------------------------------------------
# 8. Save results
# --------------------------------------------------

output_path = "results/recommendations/product_pairs"

recommendations.write.mode("overwrite").parquet(output_path)

print("\nTop 20 products frequently purchased together:")

recommendations.show(20, truncate=False)

print("Total transactions:", total_transactions)
print("Recommendation results saved to:", output_path)

spark.stop()

print("Spark session stopped.")
