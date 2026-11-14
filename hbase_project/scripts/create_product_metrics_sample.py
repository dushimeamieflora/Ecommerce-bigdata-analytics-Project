import csv
from itertools import groupby
from pathlib import Path

INPUT_FILE = Path("hbase_project/results/product_metrics_hourly.csv")
OUTPUT_FILE = Path("hbase_project/results/product_metrics_sample.csv")

MAX_RECORDS_PER_PRODUCT = 20

selected_count = 0
product_count = 0

with open(INPUT_FILE, "r", encoding="utf-8", newline="") as source:
    reader = csv.DictReader(source)

    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(
            output,
            fieldnames=reader.fieldnames
        )
        writer.writeheader()

        # The input is sorted by product, date and hour.
        for product_id, rows in groupby(
            reader,
            key=lambda row: row["product_id"]
        ):
            product_rows = list(rows)
            total = len(product_rows)

            if total <= MAX_RECORDS_PER_PRODUCT:
                selected_rows = product_rows
            else:
                # Select evenly spaced records across the timeline.
                indexes = [
                    round(
                        i * (total - 1) /
                        (MAX_RECORDS_PER_PRODUCT - 1)
                    )
                    for i in range(MAX_RECORDS_PER_PRODUCT)
                ]

                selected_rows = [
                    product_rows[index]
                    for index in indexes
                ]

            writer.writerows(selected_rows)

            selected_count += len(selected_rows)
            product_count += 1

print("\n===== PRODUCT METRICS SAMPLE =====")
print("Products represented:", product_count)
print("Selected records:", selected_count)
print("Maximum records per product:", MAX_RECORDS_PER_PRODUCT)
print("Output file:", OUTPUT_FILE)
print("Sample created successfully.")