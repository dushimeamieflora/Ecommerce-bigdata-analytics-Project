
from pathlib import Path
from io import BytesIO

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ==================================================
# 1. PATHS
# ==================================================

PROJECT_DIR = Path(__file__).resolve().parents[2]

BASE_DIR = (
    PROJECT_DIR
    / "results"
    / "integration"
    / "end_to_end"
)

# Save updated figures separately from existing PNGs.
OUTPUT_DIR = (
    PROJECT_DIR
    / "visualizations"
    / "integrated_analysis"
)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ==================================================
# 2. PLOT SETTINGS
# ==================================================

sns.set_theme(
    style="whitegrid",
    context="notebook",
    palette="deep"
)

plt.rcParams["figure.figsize"] = (11, 6)
plt.rcParams["savefig.dpi"] = 300
plt.rcParams["font.size"] = 10


# ==================================================
# 3. HELPER FUNCTIONS
# ==================================================

generated_files = []


def save_chart(filename):
    """Save the current figure as a high-resolution PNG."""

    plt.tight_layout()

    output_path = OUTPUT_DIR / filename

    try:
        # Render to memory to avoid the original
        # Matplotlib direct-file saving error.
        buffer = BytesIO()

        try:
            plt.savefig(
                buffer,
                format="png",
                dpi=300,
                bbox_inches="tight"
            )

            # Write through Python rather than
            # asking Matplotlib to open the PNG.
            output_path.write_bytes(buffer.getvalue())

        finally:
            buffer.close()

        if not output_path.is_file():
            raise RuntimeError(
                f"Output file was not created: {output_path}"
            )

        if output_path.stat().st_size == 0:
            raise RuntimeError(
                f"Output file is empty: {output_path}"
            )

        generated_files.append(output_path)

        print(f"Saved visualization: {filename}")

    finally:
        plt.close()


def spark_to_pandas(df):
    """
    Convert aggregated chart data to Pandas.
    The source datasets remain in Spark.
    """
    return df.toPandas()


def require_columns(df, columns, dataset_name):
    """Check that expected analytical columns exist."""

    missing = [
        column
        for column in columns
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            f"{dataset_name} is missing columns: {missing}"
        )


def make_product_labels(df):
    """Use product names, falling back to product IDs."""

    names = df["name"].fillna("").astype(str)
    ids = df["product_id"].astype(str)

    df["product_label"] = names.where(
        names.str.strip().ne(""),
        ids
    )

    return df


# ==================================================
# 4. SPARK SESSION
# ==================================================

spark = None

try:
    spark = (
        SparkSession.builder
        .appName("Integrated_Ecommerce_Visualizations")
        .config("spark.sql.shuffle.partitions", "8")
        .config(
            "spark.sql.execution.arrow.pyspark.enabled",
            "false"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # ==================================================
    # 5. LOAD FULL INTEGRATED DATASETS
    # ==================================================

    print("\n" + "=" * 60)
    print("LOADING FULL INTEGRATED DATASETS")
    print("=" * 60)

    products_spark = spark.read.parquet(
        str(BASE_DIR / "integrated_product_performance")
    )

    customers_spark = spark.read.parquet(
        str(BASE_DIR / "integrated_customer_journey")
    )

    categories_spark = spark.read.parquet(
        str(BASE_DIR / "integrated_category_performance")
    )

    product_count = products_spark.count()
    customer_count = customers_spark.count()
    category_count = categories_spark.count()

    print(f"\nIntegrated products: {product_count:,}")
    print(f"Integrated customers: {customer_count:,}")
    print(f"Integrated categories: {category_count:,}")

    require_columns(
        products_spark,
        [
            "product_id",
            "name",
            "transaction_revenue",
            "total_views",
            "total_cart_additions",
            "hbase_purchases"
        ],
        "Product performance"
    )

    require_columns(
        customers_spark,
        [
            "user_id",
            "total_spent",
            "browsing_events"
        ],
        "Customer journey"
    )

    require_columns(
        categories_spark,
        [
            "category_id",
            "category_name",
            "transaction_revenue"
        ],
        "Category performance"
    )

    # ==================================================
    # 6. FIGURE 1: TOP PRODUCTS BY REVENUE
    # ==================================================

    print("\n1. TOP PRODUCTS BY REVENUE")

    top_products_spark = (
        products_spark
        .select(
            "product_id",
            "name",
            F.col("transaction_revenue")
            .cast("double")
            .alias("transaction_revenue")
        )
        .filter(
            F.col("transaction_revenue").isNotNull()
        )
        .orderBy(
            F.desc("transaction_revenue"),
            F.asc("product_id")
        )
        .limit(10)
    )

    top_products = spark_to_pandas(
        top_products_spark
    )

    if not top_products.empty:

        top_products = make_product_labels(
            top_products
        )

        plt.figure(figsize=(11, 6))

        sns.barplot(
            data=top_products,
            x="transaction_revenue",
            y="product_label",
            hue="product_label",
            legend=False
        )

        plt.title(
            "Top 10 Products by Gross Transaction Revenue",
            fontsize=15,
            weight="bold"
        )

        plt.xlabel("Gross Transaction Revenue")
        plt.ylabel("Product Name")

        save_chart(
            "01_top_products_by_revenue.png"
        )

    # ==================================================
    # 7. FIGURE 2: REVENUE BY CATEGORY
    # ==================================================

    print("\n2. REVENUE BY CATEGORY")

    category_spark = (
        categories_spark
        .select(
            "category_name",
            "category_id",
            F.col("transaction_revenue")
            .cast("double")
            .alias("transaction_revenue")
        )
        .filter(
            F.col("transaction_revenue").isNotNull()
        )
        .orderBy(
            F.desc("transaction_revenue"),
            F.asc("category_id")
        )
    )

    category_df = spark_to_pandas(
        category_spark
    )

    if not category_df.empty:

        category_df["category_label"] = (
            category_df["category_name"]
            .fillna(category_df["category_id"])
            .astype(str)
        )

        plt.figure(figsize=(12, 9))

        sns.barplot(
            data=category_df,
            x="transaction_revenue",
            y="category_label",
            hue="category_label",
            legend=False
        )

        plt.title(
            "Gross Transaction Revenue by Product Category",
            fontsize=15,
            weight="bold"
        )

        plt.xlabel("Gross Transaction Revenue")
        plt.ylabel("Product Category")

        save_chart(
            "02_revenue_by_category.png"
        )

    # ==================================================
    # 8. FIGURE 3: CUSTOMER SPENDING DISTRIBUTION
    # ==================================================

    print("\n3. CUSTOMER SPENDING DISTRIBUTION")

    spending_spark = (
        customers_spark
        .select(
            F.col("total_spent")
            .cast("double")
            .alias("total_spent")
        )
        .filter(
            F.col("total_spent").isNotNull()
        )
    )

    spending_df = spark_to_pandas(
        spending_spark
    )

    if not spending_df.empty:

        spending_df = spending_df[
            spending_df["total_spent"] >= 0
        ]

        if not spending_df.empty:

            plt.figure(figsize=(11, 6))

            sns.histplot(
                data=spending_df,
                x="total_spent",
                bins=30,
                kde=True
            )

            plt.title(
                "Distribution of Total Customer Spending",
                fontsize=15,
                weight="bold"
            )

            plt.xlabel("Total Customer Spending (Net)")
            plt.ylabel("Number of Customers")

            save_chart(
                "03_customer_spending_distribution.png"
            )

    # ==================================================
    # 9. FIGURE 4: TOP CUSTOMERS BY BROWSING
    # ==================================================

    print("\n4. CUSTOMER BROWSING ACTIVITY")

    browsing_spark = (
        customers_spark
        .select(
            "user_id",
            F.col("browsing_events")
            .cast("double")
            .alias("browsing_events")
        )
        .filter(
            F.col("browsing_events").isNotNull()
        )
        .orderBy(
            F.desc("browsing_events"),
            F.asc("user_id")
        )
        .limit(10)
    )

    browsing_df = spark_to_pandas(
        browsing_spark
    )

    if not browsing_df.empty:

        browsing_df["user_id"] = (
            browsing_df["user_id"].astype(str)
        )

        plt.figure(figsize=(11, 6))

        sns.barplot(
            data=browsing_df,
            x="browsing_events",
            y="user_id",
            hue="user_id",
            legend=False
        )

        plt.title(
            "Top 10 Customers by Browsing Events",
            fontsize=15,
            weight="bold"
        )

        plt.xlabel("Number of Browsing Events")
        plt.ylabel("Customer ID")

        save_chart(
            "04_top_customers_by_browsing.png"
        )

    # ==================================================
    # 10. FIGURE 5: SAMPLED HBASE PRODUCT ACTIVITY
    # ==================================================

    print("\n5. SAMPLED HBASE PRODUCT ACTIVITY")

    activity_spark = products_spark.agg(
        F.sum(
            F.col("total_views").cast("double")
        ).alias("Views"),

        F.sum(
            F.col("total_cart_additions").cast("double")
        ).alias("Cart Additions"),

        F.sum(
            F.col("hbase_purchases").cast("double")
        ).alias("Purchases")
    )

    activity_df = spark_to_pandas(
        activity_spark
    )

    activity_long = activity_df.melt(
        var_name="Activity",
        value_name="Total"
    )

    activity_long["Total"] = pd.to_numeric(
        activity_long["Total"],
        errors="coerce"
    )

    activity_long = activity_long.dropna(
        subset=["Total"]
    )

    if not activity_long.empty:

        plt.figure(figsize=(9, 6))

        sns.barplot(
            data=activity_long,
            x="Activity",
            y="Total",
            hue="Activity",
            legend=False
        )

        plt.title(
            "Sampled HBase Product Activity Counts",
            fontsize=14,
            weight="bold"
        )

        plt.xlabel("Activity Type")
        plt.ylabel("Recorded Activity Count")

        save_chart(
            "05_hbase_product_metrics_funnel.png"
        )

        print(
            "Note: These are sampled activity totals, "
            "not a validated conversion funnel."
        )

    # ==================================================
    # 11. FIGURE 6: SAMPLED VIEWS VS FULL REVENUE
    # ==================================================

    print("\n6. PRODUCT VIEWS VS TRANSACTION REVENUE")

    scatter_spark = (
        products_spark
        .select(
            F.col("total_views")
            .cast("double")
            .alias("total_views"),

            F.col("transaction_revenue")
            .cast("double")
            .alias("transaction_revenue")
        )
        .filter(
            F.col("total_views").isNotNull()
            & F.col("transaction_revenue").isNotNull()
        )
    )

    scatter_df = spark_to_pandas(
        scatter_spark
    )

    if not scatter_df.empty:

        plt.figure(figsize=(10, 7))

        sns.scatterplot(
            data=scatter_df,
            x="total_views",
            y="transaction_revenue",
            alpha=0.65,
            s=55
        )

        plt.title(
            "Sampled Product Views vs Gross Transaction Revenue",
            fontsize=15,
            weight="bold"
        )

        plt.xlabel("Sampled HBase Product Views")
        plt.ylabel("Gross Transaction Revenue (Full Dataset)")

        save_chart(
            "06_product_views_vs_revenue.png"
        )

        print(
            "Note: Sampled views are compared with "
            "full transaction revenue. "
            "The chart does not establish causation."
        )

    # ==================================================
    # 12. FINAL SUMMARY
    # ==================================================

    print("\n" + "=" * 60)
    print("VISUALIZATION COMPLETED")
    print("=" * 60)

    print("\nFULL INTEGRATED DATASET COUNTS:")
    print(f"Products: {product_count:,}")
    print(f"Customers: {customer_count:,}")
    print(f"Categories: {category_count:,}")

    print("\nGENERATED VISUALIZATIONS:")

    for image_file in generated_files:
        print(
            f"{image_file.name} "
            f"({image_file.stat().st_size:,} bytes)"
        )

    print(f"\nTotal generated: {len(generated_files)}")

    if len(generated_files) != 6:
        raise RuntimeError(
            "Expected six visualizations, "
            f"but generated {len(generated_files)}."
        )

    print("\nOUTPUT DIRECTORY:")
    print(OUTPUT_DIR)

    print("\nAll six updated figures were saved.")

finally:
    if spark is not None:
        spark.stop()
