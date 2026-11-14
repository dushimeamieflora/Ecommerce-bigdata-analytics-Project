from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# --------------------------------------------------
# PATHS
# --------------------------------------------------

MONGO_DIR = Path("results/integration/mongodb_export")
HBASE_DIR = Path("results/integration/hbase_export")
OUTPUT_DIR = Path("results/integration/end_to_end")


# --------------------------------------------------
# SPARK SESSION
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("MongoDB_HBase_Spark_Integration")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def save_parquet(dataframe, name):
    path = str(OUTPUT_DIR / name)
    dataframe.write.mode("overwrite").parquet(path)
    print(f"Saved: {path}")


def save_csv(dataframe, name):
    path = str(OUTPUT_DIR / name)
    dataframe.coalesce(1).write.mode("overwrite").option(
        "header", True
    ).csv(path)
    print(f"Saved CSV: {path}")


def number_or_zero(column_name):
    return F.coalesce(
        F.col(column_name).cast("double"),
        F.lit(0.0)
    )


try:

    # --------------------------------------------------
    # 1. READ MONGODB DATA
    # --------------------------------------------------

    print("\nREADING MONGODB DATA")

    users = spark.read.json(str(MONGO_DIR / "users.jsonl"))
    products = spark.read.json(str(MONGO_DIR / "products.jsonl"))
    categories = spark.read.json(str(MONGO_DIR / "categories.jsonl"))
    transactions = spark.read.json(
        str(MONGO_DIR / "transactions.jsonl")
    )

    print("Users:", users.count())
    print("Products:", products.count())
    print("Categories:", categories.count())
    print("Transactions:", transactions.count())

    # --------------------------------------------------
    # 2. READ HBASE DATA
    # --------------------------------------------------

    print("\nREADING HBASE DATA")

    browsing = spark.read.json(
        str(HBASE_DIR / "browsing_events.jsonl")
    )

    hbase_metrics = spark.read.json(
        str(HBASE_DIR / "product_metrics.jsonl")
    )

    print("Browsing events:", browsing.count())
    print("HBase product metrics:", hbase_metrics.count())

    # --------------------------------------------------
    # 3. MONGODB TRANSACTION ANALYSIS
    # --------------------------------------------------

    print("\nANALYZING TRANSACTIONS")

    transaction_items = transactions.select(
        "transaction_id",
        "user_id",
        "timestamp",
        F.explode("items").alias("item")
    )

    transaction_items = transaction_items.select(
        "transaction_id",
        "user_id",
        "timestamp",
        F.col("item.product_id").alias("product_id"),
        F.col("item.quantity").cast("long").alias("quantity"),
        F.col("item.subtotal").cast("double").alias("item_revenue")
    )

    product_sales = transaction_items.groupBy(
        "product_id"
    ).agg(
        F.sum("quantity").alias("total_quantity_sold"),
        F.sum("item_revenue").alias("transaction_revenue"),
        F.countDistinct("transaction_id").alias("transaction_count"),
        F.countDistinct("user_id").alias("purchasing_customers")
    )

    customer_sales = transactions.groupBy(
        "user_id"
    ).agg(
        F.countDistinct("transaction_id").alias("total_transactions"),
        F.sum(F.col("total").cast("double")).alias("total_spent")
    )

    # --------------------------------------------------
    # 4. HBASE PRODUCT METRICS ANALYSIS
    # --------------------------------------------------

    print("\nANALYZING HBASE PRODUCT METRICS")

    metric_columns = [
        "metrics_views",
        "metrics_cart_additions",
        "metrics_purchases",
        "metrics_quantity_sold",
        "metrics_revenue"
    ]

    for column in metric_columns:
        if column not in hbase_metrics.columns:
            hbase_metrics = hbase_metrics.withColumn(
                column, F.lit(0)
            )

    metrics_clean = hbase_metrics.select(
        "product_id",
        number_or_zero("metrics_views").alias("views"),
        number_or_zero("metrics_cart_additions").alias(
            "cart_additions"
        ),
        number_or_zero("metrics_purchases").alias("hbase_purchases"),
        number_or_zero("metrics_quantity_sold").alias(
            "hbase_quantity_sold"
        ),
        number_or_zero("metrics_revenue").alias("hbase_revenue")
    )

    product_metrics_summary = metrics_clean.groupBy(
        "product_id"
    ).agg(
        F.sum("views").alias("total_views"),
        F.sum("cart_additions").alias("total_cart_additions"),
        F.sum("hbase_purchases").alias("hbase_purchases"),
        F.sum("hbase_quantity_sold").alias(
            "hbase_quantity_sold"
        ),
        F.sum("hbase_revenue").alias("hbase_revenue")
    )

    # --------------------------------------------------
    # 5. HBASE BROWSING AND SESSION ANALYSIS
    # --------------------------------------------------

    print("\nANALYZING BROWSING EVENTS AND SESSIONS")

    browsing_summary = browsing.groupBy(
        "user_id"
    ).agg(
        F.count("*").alias("browsing_events"),
        F.countDistinct("context_session_id").alias(
            "total_sessions"
        )
    )

    # Product-level browsing summary is created only
    # when the browsing records contain product IDs.
    if "event_product_id" in browsing.columns:

        browsing_products = browsing.filter(
            F.col("event_product_id").isNotNull()
        ).groupBy(
            F.col("event_product_id").alias("product_id")
        ).agg(
            F.count("*").alias("product_browsing_events"),
            F.countDistinct("user_id").alias(
                "product_browsing_users"
            )
        )

    else:
        browsing_products = None

    # --------------------------------------------------
    # 6. COMBINE MONGODB PRODUCTS AND HBASE METRICS
    # --------------------------------------------------

    print("\nCREATING INTEGRATED PRODUCT ANALYSIS")

    integrated_products = (
        products
        .join(product_sales, "product_id", "left")
        .join(product_metrics_summary, "product_id", "left")
    )

    if browsing_products is not None:
        integrated_products = integrated_products.join(
            browsing_products,
            "product_id",
            "left"
        )

    save_parquet(
        integrated_products,
        "integrated_product_performance"
    )

    # --------------------------------------------------
    # 7. COMBINE USERS, PURCHASES AND BROWSING ACTIVITY
    # --------------------------------------------------

    print("\nCREATING INTEGRATED CUSTOMER ANALYSIS")

    integrated_customers = (
        users
        .join(customer_sales, "user_id", "left")
        .join(browsing_summary, "user_id", "left")
    )

    save_parquet(
        integrated_customers,
        "integrated_customer_journey"
    )

    # --------------------------------------------------
    # 8. CATEGORY-LEVEL ANALYSIS
    # --------------------------------------------------

    print("\nCREATING CATEGORY PERFORMANCE ANALYSIS")

    if "category_id" in products.columns:
        category_products = integrated_products.join(
            categories.select(
                "category_id",
                F.col("name").alias("category_name")
            ),
            "category_id",
            "left"
        )

        category_performance = category_products.groupBy(
            "category_id",
            "category_name"
        ).agg(
            F.sum("transaction_revenue").alias(
                "transaction_revenue"
            ),
            F.sum("total_quantity_sold").alias(
                "total_quantity_sold"
            ),
            F.sum("total_views").alias("total_views"),
            F.sum("total_cart_additions").alias(
                "total_cart_additions"
            )
        )

        save_parquet(
            category_performance,
            "integrated_category_performance"
        )

        save_csv(
            category_performance,
            "integrated_category_performance_csv"
        )

    # --------------------------------------------------
    # 9. SAVE OTHER ANALYTICAL RESULTS
    # --------------------------------------------------

    save_csv(
        integrated_products,
        "integrated_product_performance_csv"
    )

    save_csv(
        integrated_customers,
        "integrated_customer_journey_csv"
    )

    # --------------------------------------------------
    # 10. DISPLAY RESULTS
    # --------------------------------------------------

    print("\nTOP PRODUCTS BY TRANSACTION REVENUE")

    integrated_products.orderBy(
        F.col("transaction_revenue").desc_nulls_last()
    ).select(
        "product_id",
        "name",
        "transaction_revenue",
        "total_quantity_sold",
        "total_views",
        "total_cart_additions"
    ).show(10, truncate=False)

    print("\nTOP CUSTOMERS BY TOTAL SPENDING")

    integrated_customers.orderBy(
        F.col("total_spent").desc_nulls_last()
    ).select(
        "user_id",
        "total_transactions",
        "total_spent",
        "browsing_events",
        "total_sessions"
    ).show(10, truncate=False)

    print("\nEND-TO-END MONGODB + HBASE + SPARK ANALYSIS COMPLETED.")

    print(f"Results saved in: {OUTPUT_DIR}")

finally:
    spark.stop()