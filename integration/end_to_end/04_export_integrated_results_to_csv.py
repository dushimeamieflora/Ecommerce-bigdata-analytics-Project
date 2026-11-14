from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StructType


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

INPUT_DIR = Path("results/integration/end_to_end")
OUTPUT_DIR = Path("results/integration/end_to_end")

DATASETS = [
    "integrated_product_performance",
    "integrated_customer_journey",
    "integrated_category_performance"
]


# --------------------------------------------------
# SPARK SESSION
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Export_Integrated_Results_To_CSV")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# CONVERT COMPLEX COLUMNS
# --------------------------------------------------

def prepare_for_csv(df):

    for field in df.schema.fields:

        column_name = field.name
        data_type = field.dataType

        if isinstance(data_type, (ArrayType, MapType, StructType)):
            df = df.withColumn(
                column_name,
                F.to_json(F.col(column_name))
            )

    return df


# --------------------------------------------------
# EXPORT DATASETS
# --------------------------------------------------

try:

    for dataset in DATASETS:

        print(f"\nExporting: {dataset}")

        parquet_path = INPUT_DIR / dataset
        csv_path = OUTPUT_DIR / f"{dataset}_csv"

        df = spark.read.parquet(str(parquet_path))

        print(f"Records: {df.count():,}")

        df = prepare_for_csv(df)

        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .option("encoding", "UTF-8")
            .csv(str(csv_path))
        )

        print(f"CSV export successful: {csv_path}")

    print("\nALL INTEGRATED CSV EXPORTS COMPLETED SUCCESSFULLY.")

finally:
    spark.stop()