
import json
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    ArrayType,
    MapType
)
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    to_timestamp,
    when,
    count,
    sum as spark_sum,
    transform,
    filter as spark_filter,
    transform_values,
    struct,
    size
)

# --------------------------------------------------
# 1. PROJECT PATHS
# --------------------------------------------------

PROJECT_DIR = Path(__file__).resolve().parent.parent

CHUNK_DIR = PROJECT_DIR / "data" / "temp" / "sessions"
OUTPUT_DIR = PROJECT_DIR / "data" / "processed" / "sessions"
QUALITY_DIR = PROJECT_DIR / "results" / "data_quality"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
QUALITY_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# 2. LOGGING
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# --------------------------------------------------
# 3. DEFINE EXPLICIT JSON SCHEMA
# --------------------------------------------------

geo_schema = StructType([
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("ip_address", StringType(), True)
])

device_schema = StructType([
    StructField("type", StringType(), True),
    StructField("os", StringType(), True),
    StructField("browser", StringType(), True)
])

page_view_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("page_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("view_duration", IntegerType(), True)
])

cart_item_schema = StructType([
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

session_schema = StructType([
    StructField("session_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
    StructField("duration_seconds", LongType(), True),
    StructField("geo_data", geo_schema, True),
    StructField("device_profile", device_schema, True),
    StructField("viewed_products", ArrayType(StringType()), True),
    StructField("page_views", ArrayType(page_view_schema), True),
    StructField(
        "cart_contents",
        MapType(StringType(), cart_item_schema),
        True
    ),
    StructField("conversion_status", StringType(), True),
    StructField("referrer", StringType(), True)
])

# --------------------------------------------------
# 4. START SPARK
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Ecommerce_Session_Cleaning")
    .master("local[1]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.default.parallelism", "1")
    .config("spark.sql.files.maxPartitionBytes", "8388608")
    .config("spark.ui.enabled", "false")
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# --------------------------------------------------
# 5. FIND SESSION CHUNKS
# --------------------------------------------------

chunk_files = sorted(
    CHUNK_DIR.glob("sessions_*_chunk_*.jsonl")
)

logging.info("Found %s session chunks.", len(chunk_files))

if not chunk_files:
    raise FileNotFoundError(
        "No session chunks found. Run split_sessions.py first."
    )

# --------------------------------------------------
# 6. PROCESS ONE CHUNK AT A TIME
# --------------------------------------------------

quality_results = []
total_input = 0
total_cleaned = 0

try:

    for index, file_path in enumerate(chunk_files, start=1):

        batch_name = f"batch_{index:03d}"
        batch_output = OUTPUT_DIR / batch_name

        logging.info(
            "Processing chunk %s/%s: %s",
            index,
            len(chunk_files),
            file_path.name
        )

        # --------------------------------------------------
        # 6A. READ ONE CHUNK
        # --------------------------------------------------

        df = (
            spark.read
            .schema(session_schema)
            .json(str(file_path))
        )

        original_columns = df.columns

        input_rows = df.count()
        total_input += input_rows

        # --------------------------------------------------
        # 6B. CLEAN SESSION IDENTIFIERS
        # --------------------------------------------------

        df = df.withColumn(
            "session_id",
            trim(col("session_id"))
        )

        df = df.withColumn(
            "user_id",
            trim(col("user_id"))
        )

        # --------------------------------------------------
        # 6C. CLEAN SESSION TIMESTAMPS
        # --------------------------------------------------

        df = df.withColumn(
            "start_time",
            to_timestamp(col("start_time"))
        )

        df = df.withColumn(
            "end_time",
            to_timestamp(col("end_time"))
        )

        # --------------------------------------------------
        # 6D. CLEAN GEO DATA
        # --------------------------------------------------

        df = df.withColumn(
            "geo_data",
            when(
                col("geo_data").isNotNull(),
                struct(
                    trim(col("geo_data.city")).alias("city"),
                    trim(col("geo_data.state")).alias("state"),
                    trim(col("geo_data.country")).alias("country"),
                    trim(col("geo_data.ip_address")).alias("ip_address")
                )
            ).otherwise(None)
        )

        # --------------------------------------------------
        # 6E. CLEAN DEVICE PROFILE
        # --------------------------------------------------

        df = df.withColumn(
            "device_profile",
            when(
                col("device_profile").isNotNull(),
                struct(
                    trim(col("device_profile.type")).alias("type"),
                    trim(col("device_profile.os")).alias("os"),
                    trim(col("device_profile.browser")).alias("browser")
                )
            ).otherwise(None)
        )

        # --------------------------------------------------
        # 6F. CLEAN VIEWED PRODUCTS
        # --------------------------------------------------

        df = df.withColumn(
            "viewed_products",
            when(
                col("viewed_products").isNotNull(),
                spark_filter(
                    transform(
                        col("viewed_products"),
                        lambda x: trim(x)
                    ),
                    lambda x: x.isNotNull() & (x != "")
                )
            ).otherwise(None)
        )

        # --------------------------------------------------
        # 6G. CLEAN NESTED PAGE VIEWS
        # --------------------------------------------------

        # Null product_id and category_id values are preserved.
        # These can be legitimate for home and other page types.

        df = df.withColumn(
            "page_views",
            when(
                col("page_views").isNotNull(),
                spark_filter(
                    transform(
                        col("page_views"),
                        lambda x: struct(
                            to_timestamp(
                                x["timestamp"]
                            ).alias("timestamp"),

                            lower(
                                trim(x["page_type"])
                            ).alias("page_type"),

                            trim(
                                x["product_id"]
                            ).alias("product_id"),

                            trim(
                                x["category_id"]
                            ).alias("category_id"),

                            when(
                                x["view_duration"] >= 0,
                                x["view_duration"]
                            ).otherwise(None).alias("view_duration")
                        )
                    ),
                    lambda x: (
                        x["timestamp"].isNotNull()
                        & x["page_type"].isNotNull()
                        & (x["page_type"] != "")
                    )
                )
            ).otherwise(None)
        )

        # --------------------------------------------------
        # 6H. CLEAN CART CONTENTS
        # --------------------------------------------------

        # Product IDs remain the map keys.
        # Invalid quantities and prices become null.

        df = df.withColumn(
            "cart_contents",
            when(
                col("cart_contents").isNotNull(),
                transform_values(
                    col("cart_contents"),
                    lambda key, item: struct(
                        when(
                            item["quantity"] > 0,
                            item["quantity"]
                        ).otherwise(None).alias("quantity"),

                        when(
                            item["price"] >= 0,
                            item["price"]
                        ).otherwise(None).alias("price")
                    )
                )
            ).otherwise(None)
        )

        # --------------------------------------------------
        # 6I. STANDARDIZE STATUS AND REFERRER
        # --------------------------------------------------

        for column_name in [
            "conversion_status",
            "referrer"
        ]:
            df = df.withColumn(
                column_name,
                when(
                    col(column_name).isNull()
                    | (trim(col(column_name)) == ""),
                    "Unknown"
                ).otherwise(trim(col(column_name)))
            )

        # --------------------------------------------------
        # 6J. REMOVE INVALID SESSION RECORDS
        # --------------------------------------------------

        # Require valid session ID, user ID and start time.

        df = df.filter(
            col("session_id").isNotNull()
            & (col("session_id") != "")
            & col("user_id").isNotNull()
            & (col("user_id") != "")
            & col("start_time").isNotNull()
        )

        # Remove negative session durations.

        df = df.filter(
            col("duration_seconds").isNull()
            | (col("duration_seconds") >= 0)
        )

        # Reject end times before start times.

        df = df.filter(
            col("end_time").isNull()
            | (col("end_time") >= col("start_time"))
        )

        # --------------------------------------------------
        # 6K. CALCULATE QUALITY MEASUREMENTS
        # --------------------------------------------------

        aggregate_expressions = [
            count("*").alias("cleaned_rows"),

            spark_sum(
                when(
                    col("page_views").isNotNull(),
                    size(col("page_views"))
                ).otherwise(0)
            ).alias("page_view_count"),

            spark_sum(
                when(
                    col("cart_contents").isNotNull(),
                    size(col("cart_contents"))
                ).otherwise(0)
            ).alias("cart_item_count")
        ]

        for column_name in original_columns:
            aggregate_expressions.append(
                spark_sum(
                    when(
                        col(column_name).isNull(),
                        1
                    ).otherwise(0)
                ).alias(f"null_{column_name}")
            )

        quality_row = df.agg(
            *aggregate_expressions
        ).first()

        cleaned_rows = quality_row["cleaned_rows"]
        total_cleaned += cleaned_rows

        # --------------------------------------------------
        # 6L. SAVE CLEANED CHUNK AS PARQUET
        # --------------------------------------------------

        df.write.mode("overwrite").parquet(
            str(batch_output)
        )

        batch_quality = {
            "source_file": file_path.name,
            "output_folder": batch_name,
            "input_rows": input_rows,
            "cleaned_rows": cleaned_rows,
            "removed_session_rows": input_rows - cleaned_rows,

            "page_view_count": (
                quality_row["page_view_count"] or 0
            ),

            "cart_item_count": (
                quality_row["cart_item_count"] or 0
            ),

            "missing_top_level_values": {
                column_name: quality_row[
                    f"null_{column_name}"
                ]
                for column_name in original_columns
            }
        }

        quality_results.append(batch_quality)

        logging.info(
            "Completed chunk %s/%s: %s cleaned rows.",
            index,
            len(chunk_files),
            cleaned_rows
        )

        del df
        del quality_row

    # --------------------------------------------------
    # 7. SAVE DATA QUALITY REPORT
    # --------------------------------------------------

    report = {
        "dataset": "sessions",
        "source_chunks_processed": len(chunk_files),
        "total_input_rows": total_input,
        "total_cleaned_rows": total_cleaned,
        "total_removed_session_rows": (
            total_input - total_cleaned
        ),
        "batch_results": quality_results
    }

    report_path = (
        QUALITY_DIR / "sessions_quality.json"
    )

    with report_path.open(
        "w",
        encoding="utf-8"
    ) as report_file:

        json.dump(
            report,
            report_file,
            indent=4,
            ensure_ascii=False
        )

    logging.info("All session chunks completed.")
    logging.info("Total input rows: %s", total_input)
    logging.info("Total cleaned rows: %s", total_cleaned)

    logging.info(
        "Total removed session rows: %s",
        total_input - total_cleaned
    )

    logging.info(
        "Quality report saved to: %s",
        report_path
    )

    logging.info(
        "Parquet output saved under: %s",
        OUTPUT_DIR
    )

finally:
    spark.stop()
