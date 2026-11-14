
import json
from pathlib import Path

from pymongo import MongoClient

# Project paths
PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "data" / "raw"

# MongoDB connection
client = MongoClient("mongodb://localhost:27017")
client.admin.command("ping")

db = client["ecommerce_analytics"]
collection = db["transactions"]

# Find all transaction JSON Lines files
transaction_files = sorted(DATA_DIR.glob("transactions_*.jsonl"))

if not transaction_files:
    raise FileNotFoundError(
        f"No transaction JSONL files found in {DATA_DIR}"
    )

# Prevent accidental duplicate imports
existing_count = collection.count_documents({})

if existing_count > 0:
    raise RuntimeError(
        f"The transactions collection already contains "
        f"{existing_count} documents. Import stopped to avoid duplicates."
    )

BATCH_SIZE = 5000
batch = []
total_inserted = 0

print(f"Found {len(transaction_files)} transaction files.")
print("Starting transaction import...")

try:
    for file_path in transaction_files:
        print(f"\nReading: {file_path.name}")

        with file_path.open("r", encoding="utf-8") as file:
            for line in file:
                transaction = json.loads(line)
                batch.append(transaction)

                if len(batch) >= BATCH_SIZE:
                    result = collection.insert_many(
                        batch,
                        ordered=False
                    )
                    total_inserted += len(result.inserted_ids)
                    batch.clear()

                    print(f"Inserted: {total_inserted:,}")

    # Insert the remaining records
    if batch:
        result = collection.insert_many(batch, ordered=False)
        total_inserted += len(result.inserted_ids)
        batch.clear()

    # Create indexes for future analytical queries
    collection.create_index("transaction_id", unique=True)
    collection.create_index("user_id")
    collection.create_index("timestamp")
    collection.create_index("items.product_id")

    print("\nTRANSACTION IMPORT COMPLETED")
    print(f"Total inserted: {total_inserted:,}")
    print(f"Documents in collection: {collection.count_documents({}):,}")
    print("Indexes created successfully.")

finally:
    client.close()
