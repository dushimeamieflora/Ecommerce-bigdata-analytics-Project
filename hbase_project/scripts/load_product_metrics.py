
import csv
import happybase
import os
import time

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

CSV_PATH = "hbase_project/results/product_metrics_sample.csv"
TABLE_NAME = "ecom:product_metrics"

HOST = "localhost"
PORT = 9090
TIMEOUT = 120000
BATCH_SIZE = 50

# --------------------------------------------------
# LOAD DATA INTO HBASE
# --------------------------------------------------

def load_product_metrics():

    if not os.path.exists(CSV_PATH):
        print("ERROR: CSV file not found:", CSV_PATH)
        return

    connection = None
    total_loaded = 0
    start_time = time.time()

    try:
        print("Connecting to HBase...")

        connection = happybase.Connection(
            host=HOST,
            port=PORT,
            timeout=TIMEOUT
        )

        connection.open()

        table = connection.table(TABLE_NAME)

        print("Connected successfully.")
        print("Target table:", TABLE_NAME)
        print("Batch size:", BATCH_SIZE)
        print("\nStarting data loading...\n")

        with open(CSV_PATH, "r", encoding="utf-8", newline="") as file:

            reader = csv.DictReader(file)

            batch = table.batch(batch_size=BATCH_SIZE)

            for row in reader:

                # Deterministic row key
                hour = str(row["hour"]).zfill(2)

                row_key = (
                    f'{row["product_id"]}'
                    f'#{row["date"]}'
                    f'#{hour}'
                ).encode("utf-8")

                # HBase column values
                data = {
                    b"metrics:views": row["views"].encode("utf-8"),
                    b"metrics:cart_additions": row["cart_additions"].encode("utf-8"),
                    b"metrics:purchases": row["purchases"].encode("utf-8"),
                    b"metrics:quantity_sold": row["quantity_sold"].encode("utf-8"),
                    b"metrics:revenue": row["revenue"].encode("utf-8"),
                }

                batch.put(row_key, data)
                total_loaded += 1

                if total_loaded % 1000 == 0:
                    elapsed = time.time() - start_time

                    print(
                        f"Prepared {total_loaded:,} records | "
                        f"Elapsed: {elapsed:.1f} seconds"
                    )

            # Send any remaining records
            batch.send()

        elapsed = time.time() - start_time

        print("\n===================================")
        print("PRODUCT METRICS LOADING COMPLETED")
        print("===================================")
        print(f"Records processed: {total_loaded:,}")
        print(f"Elapsed time: {elapsed:.2f} seconds")
        print("HBase table:", TABLE_NAME)

    except Exception as error:

        print("\nLOADING FAILED")
        print(type(error).__name__, error)
        print(f"Records prepared before failure: {total_loaded:,}")
        print(
            "You can safely rerun this loader: "
            "the deterministic row keys make repeated writes idempotent."
        )

    finally:

        if connection:
            connection.close()
            print("\nHBase connection closed.")


if __name__ == "__main__":
    load_product_metrics()
