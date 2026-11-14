
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# ==================================================
# 1. START SPARK
# ==================================================

spark = (
    SparkSession.builder
    .appName("Ecommerce_Spark_SQL_Analytics")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ==================================================
# 2. LOAD CLEANED PARQUET DATASETS
# ==================================================

users = spark.read.parquet("data/processed/users")
products = spark.read.parquet("data/processed/products")
categories = spark.read.parquet("data/processed/categories")
transactions = spark.read.parquet("data/processed/transactions")

# ==================================================
# 3. EXPLODE TRANSACTION ITEMS
# Each transaction may contain multiple products.
# ==================================================

transaction_items = (
    transactions
    .select(
        "transaction_id",
        "user_id",
        "timestamp",
        "status",
        explode("items").alias("item")
    )
    .select(
        "transaction_id",
        "user_id",
        "timestamp",
        "status",
        col("item.product_id").alias("product_id"),
        col("item.quantity").alias("quantity"),
        col("item.unit_price").alias("unit_price"),
        col("item.subtotal").alias("item_subtotal")
    )
)

# ==================================================
# 4. REGISTER TEMPORARY SQL VIEWS
# ==================================================

users.createOrReplaceTempView("users")
products.createOrReplaceTempView("products")
categories.createOrReplaceTempView("categories")
transactions.createOrReplaceTempView("transactions")
transaction_items.createOrReplaceTempView("transaction_items")

# ==================================================
# QUERY 1: REVENUE BY PRODUCT CATEGORY
# SQL concepts: INNER JOIN, GROUP BY, SUM,
# COUNT DISTINCT and ORDER BY.
# ==================================================

category_revenue = spark.sql("""
    SELECT
        c.category_id,
        c.name AS category_name,
        COUNT(DISTINCT ti.transaction_id) AS total_transactions,
        SUM(ti.quantity) AS total_quantity_sold,
        ROUND(SUM(ti.item_subtotal), 2) AS total_revenue
    FROM transaction_items ti
    INNER JOIN products p
        ON ti.product_id = p.product_id
    INNER JOIN categories c
        ON p.category_id = c.category_id
    WHERE ti.status IN ('completed', 'delivered', 'shipped')
    GROUP BY
        c.category_id,
        c.name
    ORDER BY total_revenue DESC
""")

print("\n" + "=" * 65)
print("QUERY 1: REVENUE BY PRODUCT CATEGORY")
print("=" * 65)

category_revenue.show(25, truncate=False)

# ==================================================
# QUERY 2: MONTHLY REVENUE AND REVENUE GROWTH
# SQL concepts: CTE, LAG window function,
# aggregation and NULLIF.
# ==================================================

monthly_revenue = spark.sql("""
    WITH monthly_sales AS (
        SELECT
            DATE_FORMAT(timestamp, 'yyyy-MM') AS sales_month,
            SUM(total) AS revenue,
            COUNT(DISTINCT transaction_id) AS total_transactions
        FROM transactions
        WHERE status IN ('completed', 'delivered', 'shipped')
        GROUP BY DATE_FORMAT(timestamp, 'yyyy-MM')
    ),

    revenue_comparison AS (
        SELECT
            sales_month,
            revenue,
            total_transactions,
            LAG(revenue) OVER (
                ORDER BY sales_month
            ) AS previous_month_revenue
        FROM monthly_sales
    )

    SELECT
        sales_month,
        ROUND(revenue, 2) AS revenue,
        total_transactions,
        ROUND(previous_month_revenue, 2)
            AS previous_month_revenue,
        ROUND(
            (revenue - previous_month_revenue)
            / NULLIF(previous_month_revenue, 0) * 100,
            2
        ) AS revenue_growth_percentage
    FROM revenue_comparison
    ORDER BY sales_month
""")

print("\n" + "=" * 65)
print("QUERY 2: MONTHLY REVENUE AND GROWTH")
print("=" * 65)

monthly_revenue.show(50, truncate=False)

# ==================================================
# QUERY 3: TOP 10 CUSTOMERS BY TOTAL SPENDING
# SQL concepts: CTE, INNER JOIN, GROUP BY,
# RANK window function.
# ==================================================

top_customers = spark.sql("""
    WITH customer_spending AS (
        SELECT
            user_id,
            COUNT(DISTINCT transaction_id) AS total_orders,
            SUM(total) AS total_spending
        FROM transactions
        WHERE status IN ('completed', 'delivered', 'shipped')
        GROUP BY user_id
    ),

    ranked_customers AS (
        SELECT
            u.user_id,
            u.gender,
            cs.total_orders,
            ROUND(cs.total_spending, 2) AS total_spending,
            RANK() OVER (
                ORDER BY cs.total_spending DESC
            ) AS spending_rank
        FROM customer_spending cs
        INNER JOIN users u
            ON cs.user_id = u.user_id
    )

    SELECT *
    FROM ranked_customers
    WHERE spending_rank <= 10
    ORDER BY spending_rank
""")

print("\n" + "=" * 65)
print("QUERY 3: TOP 10 CUSTOMERS BY SPENDING")
print("=" * 65)

top_customers.show(10, truncate=False)

# ==================================================
# QUERY 4: TOP 10 BEST-SELLING PRODUCTS
# SQL concepts: multiple INNER JOINs,
# CTE, SUM, GROUP BY and DENSE_RANK.
# ==================================================

top_products = spark.sql("""
    WITH product_sales AS (
        SELECT
            p.product_id,
            p.name AS product_name,
            c.name AS category_name,
            SUM(ti.quantity) AS units_sold,
            SUM(ti.item_subtotal) AS product_revenue
        FROM transaction_items ti
        INNER JOIN products p
            ON ti.product_id = p.product_id
        INNER JOIN categories c
            ON p.category_id = c.category_id
        WHERE ti.status IN ('completed', 'delivered', 'shipped')
        GROUP BY
            p.product_id,
            p.name,
            c.name
    ),

    ranked_products AS (
        SELECT
            *,
            DENSE_RANK() OVER (
                ORDER BY units_sold DESC
            ) AS sales_rank
        FROM product_sales
    )

    SELECT
        product_id,
        product_name,
        category_name,
        units_sold,
        ROUND(product_revenue, 2) AS product_revenue,
        sales_rank
    FROM ranked_products
    WHERE sales_rank <= 10
    ORDER BY sales_rank
""")

print("\n" + "=" * 65)
print("QUERY 4: TOP 10 BEST-SELLING PRODUCTS")
print("=" * 65)

top_products.show(10, truncate=False)

# ==================================================
# 5. SAVE ALL FOUR ANALYTICAL RESULTS
# ==================================================

category_revenue.write.mode("overwrite").parquet(
    "results/spark_sql/category_revenue"
)

monthly_revenue.write.mode("overwrite").parquet(
    "results/spark_sql/monthly_revenue"
)

top_customers.write.mode("overwrite").parquet(
    "results/spark_sql/top_customers"
)

top_products.write.mode("overwrite").parquet(
    "results/spark_sql/top_products"
)

# ==================================================
# 6. COMPLETE
# ==================================================

print("\n" + "=" * 65)
print("ALL SPARK SQL QUERIES COMPLETED SUCCESSFULLY!")
print("Results saved in results/spark_sql/")
print("=" * 65)

spark.stop()
