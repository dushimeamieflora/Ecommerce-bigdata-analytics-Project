
import json
import csv
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# Project folders
PROJECT_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_DIR / "data" / "raw"
OUTPUT_DIR = PROJECT_DIR / "hbase_project" / "results"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Valid transaction statuses
VALID_STATUSES = {"completed", "shipped", "delivered"}

# Product metrics:
# key = (product_id, date, hour)
metrics = defaultdict(lambda: {
    "views": 0,
    "cart_additions": 0,
    "purchases": 0,
    "quantity_sold": 0,
    "revenue": 0.0
})


def get_time_parts(timestamp):
    """Extract date and hour from an ISO timestamp."""
    dt = datetime.fromisoformat(timestamp)
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H")


def process_sessions():
    print("Processing session files...")

    session_files = sorted(RAW_DIR.glob("sessions_*.jsonl"))
    processed = 0

    for filepath in session_files:
        if filepath.stat().st_size == 0:
            continue

        print(f"Reading {filepath.name}")

        with open(filepath, "r", encoding="utf-8") as file:
            for line in file:
                if not line.strip():
                    continue

                session = json.loads(line)

                # 1. Product views
                for event in session.get("page_views", []):
                    if (
                        event.get("page_type") == "product_detail"
                        and event.get("product_id")
                        and event.get("timestamp")
                    ):
                        product_id = event["product_id"]
                        date, hour = get_time_parts(event["timestamp"])

                        key = (product_id, date, hour)
                        metrics[key]["views"] += 1

                # 2. Cart additions (session cart snapshot)
                if session.get("end_time"):
                    date, hour = get_time_parts(session["end_time"])

                    for product_id, cart_item in session.get(
                        "cart_contents", {}
                    ).items():

                        quantity = int(cart_item.get("quantity", 0))

                        key = (product_id, date, hour)
                        metrics[key]["cart_additions"] += quantity

        processed += 1

    print(f"Session files processed: {processed}")


def process_transactions():
    print("\nProcessing transaction files...")

    transaction_files = sorted(
        RAW_DIR.glob("transactions_*.jsonl")
    )
    processed = 0
    skipped_statuses = defaultdict(int)

    for filepath in transaction_files:
        if filepath.stat().st_size == 0:
            continue

        print(f"Reading {filepath.name}")

        with open(filepath, "r", encoding="utf-8") as file:
            for line in file:
                if not line.strip():
                    continue

                transaction = json.loads(line)

                status = transaction.get("status", "").lower()

                if status not in VALID_STATUSES:
                    skipped_statuses[status] += 1
                    continue

                if not transaction.get("timestamp"):
                    continue

                date, hour = get_time_parts(
                    transaction["timestamp"]
                )

                for item in transaction.get("items", []):
                    product_id = item.get("product_id")

                    if not product_id:
                        continue

                    key = (product_id, date, hour)

                    metrics[key]["purchases"] += 1
                    metrics[key]["quantity_sold"] += int(
                        item.get("quantity", 0)
                    )
                    metrics[key]["revenue"] += float(
                        item.get("subtotal", 0)
                    )

        processed += 1

    print(f"Transaction files processed: {processed}")

    if skipped_statuses:
        print("Skipped transaction statuses:", dict(skipped_statuses))


def save_results():
    output_file = OUTPUT_DIR / "product_metrics_hourly.csv"

    print("\nSaving aggregated metrics...")

    with open(
        output_file,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.writer(file)

        writer.writerow([
            "product_id",
            "date",
            "hour",
            "views",
            "cart_additions",
            "purchases",
            "quantity_sold",
            "revenue"
        ])

        for (product_id, date, hour), values in sorted(
            metrics.items()
        ):

            writer.writerow([
                product_id,
                date,
                hour,
                values["views"],
                values["cart_additions"],
                values["purchases"],
                values["quantity_sold"],
                round(values["revenue"], 2)
            ])

    print(f"Output saved to: {output_file}")
    print(f"Total product-time records: {len(metrics)}")


def show_summary():
    totals = {
        "views": 0,
        "cart_additions": 0,
        "purchases": 0,
        "quantity_sold": 0,
        "revenue": 0.0
    }

    for values in metrics.values():
        for name in totals:
            totals[name] += values[name]

    print("\n" + "=" * 50)
    print("PRODUCT METRICS SUMMARY")
    print("=" * 50)

    for name, value in totals.items():
        if name == "revenue":
            print(f"{name}: {value:,.2f}")
        else:
            print(f"{name}: {value:,}")

    print("=" * 50)


if __name__ == "__main__":
    process_sessions()
    process_transactions()
    save_results()
    show_summary()
