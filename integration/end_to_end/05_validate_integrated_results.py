from pathlib import Path
import csv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# --------------------------------------------------
# PATHS
# --------------------------------------------------

BASE_DIR = Path("results/integration/end_to_end")
REPORT_DIR = Path("results/integration/validation")

REPORT_DIR.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------
# SPARK
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Validate_MongoDB_HBase_Spark_Integration")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

report = []


def record(label, value):
    report.append((label, str(value)))
    print(f"{label}: {value}")


def duplicate_key_groups(df, key):
    if key not in df.columns:
        return "Column not found"

    return (
        df.groupBy(key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def null_key_count(df, key):
    if key not in df.columns:
        return "Column not found"

    return df.filter(
        F.col(key).isNull()
    ).count()


def count_matching_rows(df, column):
    if column not in df.columns:
        return "Column not found"

    return df.filter(
        F.col(column).isNotNull()
    ).count()


def sum_column(df, column):
    if column not in df.columns:
        return "Column not found"

    result = df.agg(
        F.sum(F.col(column).cast("double")).alias("total")
    ).first()["total"]

    return 0 if result is None else round(result, 2)


try:

    print("\nREADING INTEGRATED PARQUET DATA")

    products = spark.read.parquet(
        str(BASE_DIR / "integrated_product_performance")
    )

    customers = spark.read.parquet(
        str(BASE_DIR / "integrated_customer_journey")
    )

    categories = spark.read.parquet(
        str(BASE_DIR / "integrated_category_performance")
    )

    # --------------------------------------------------
    # 1. RECORD COUNTS
    # --------------------------------------------------

    print("\n1. RECORD COUNTS")

    record("Integrated products", products.count())
    record("Integrated customers", customers.count())
    record("Integrated categories", categories.count())

    # --------------------------------------------------
    # 2. PRIMARY KEY VALIDATION
    # --------------------------------------------------

    print("\n2. PRIMARY KEY VALIDATION")

    record(
        "Product duplicate key groups",
        duplicate_key_groups(products, "product_id")
    )

    record(
        "Customer duplicate key groups",
        duplicate_key_groups(customers, "user_id")
    )

    record(
        "Category duplicate key groups",
        duplicate_key_groups(categories, "category_id")
    )

    record(
        "Products with missing product_id",
        null_key_count(products, "product_id")
    )

    record(
        "Customers with missing user_id",
        null_key_count(customers, "user_id")
    )

    # --------------------------------------------------
    # 3. JOIN COVERAGE
    # --------------------------------------------------

    print("\n3. JOIN COVERAGE")

    record(
        "Products matched to MongoDB sales",
        count_matching_rows(products, "transaction_revenue")
    )

    record(
        "Products matched to HBase metrics",
        count_matching_rows(products, "total_views")
    )

    record(
        "Customers matched to transactions",
        count_matching_rows(customers, "total_transactions")
    )

    record(
        "Customers matched to HBase browsing",
        count_matching_rows(customers, "browsing_events")
    )

    record(
        "Categories with transaction revenue",
        count_matching_rows(categories, "transaction_revenue")
    )

    # --------------------------------------------------
    # 4. BUSINESS MEASURES
    # --------------------------------------------------

    print("\n4. BUSINESS MEASURES")

    record(
        "Total product transaction revenue",
        sum_column(products, "transaction_revenue")
    )

    record(
        "Total quantity sold",
        sum_column(products, "total_quantity_sold")
    )

    record(
        "Total HBase product views in exported sample",
        sum_column(products, "total_views")
    )

    record(
        "Total HBase cart additions in exported sample",
        sum_column(products, "total_cart_additions")
    )

    record(
        "Total customer spending",
        sum_column(customers, "total_spent")
    )

    record(
        "Total customer transactions",
        sum_column(customers, "total_transactions")
    )

    record(
        "Total customer browsing events",
        sum_column(customers, "browsing_events")
    )

    record(
        "Total customer sessions",
        sum_column(customers, "total_sessions")
    )

    # --------------------------------------------------
    # 5. NEGATIVE VALUE CHECKS
    # --------------------------------------------------

    print("\n5. NEGATIVE VALUE CHECKS")

    for df, label, column in [
        (products, "Negative product revenue rows", "transaction_revenue"),
        (products, "Negative product quantities", "total_quantity_sold"),
        (customers, "Negative customer spending rows", "total_spent"),
        (categories, "Negative category revenue rows", "transaction_revenue")
    ]:

        if column in df.columns:
            negative_count = df.filter(
                F.col(column).cast("double") < 0
            ).count()

            record(label, negative_count)

    # --------------------------------------------------
    # 6. SAVE REPORT
    # --------------------------------------------------

    report_txt = REPORT_DIR / "integration_validation_report.txt"
    report_csv = REPORT_DIR / "integration_validation_report.csv"

    with report_txt.open("w", encoding="utf-8") as file:
        file.write(
            "MONGODB + HBASE + SPARK INTEGRATION VALIDATION\n"
        )
        file.write("=" * 55 + "\n\n")

        for label, value in report:
            file.write(f"{label}: {value}\n")

    with report_csv.open(
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.writer(file)
        writer.writerow(["Validation check", "Result"])
        writer.writerows(report)

    print("\nVALIDATION REPORTS SAVED")
    print(report_txt)
    print(report_csv)

finally:
    spark.stop()