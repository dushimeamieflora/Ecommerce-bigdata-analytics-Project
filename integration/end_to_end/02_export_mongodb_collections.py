import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from bson import ObjectId
from bson.decimal128 import Decimal128
from bson.binary import Binary
from pymongo import MongoClient


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "ecommerce_analytics"

COLLECTIONS = [
    "users",
    "products",
    "categories",
    "transactions"
]

OUTPUT_DIR = Path("results/integration/mongodb_export")
BATCH_SIZE = 5000


# --------------------------------------------------
# NORMALIZE MONGODB DOCUMENTS
# --------------------------------------------------

def normalize(value):

    if isinstance(value, ObjectId):
        return str(value)

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, Decimal128):
        return float(value.to_decimal())

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, Binary):
        return value.hex()

    if isinstance(value, bytes):
        return value.hex()

    if isinstance(value, dict):
        return {
            str(key): normalize(item)
            for key, item in value.items()
        }

    if isinstance(value, list):
        return [normalize(item) for item in value]

    return value


# --------------------------------------------------
# EXPORT COLLECTION
# --------------------------------------------------

def export_collection(database, collection_name):

    collection = database[collection_name]

    output_file = OUTPUT_DIR / f"{collection_name}.jsonl"

    total = 0

    print(f"\nExporting collection: {collection_name}")
    print(f"Documents available: {collection.count_documents({}):,}")

    with output_file.open("w", encoding="utf-8") as file:

        cursor = collection.find(
            {},
            no_cursor_timeout=True
        ).batch_size(BATCH_SIZE)

        try:
            for document in cursor:

                clean_document = normalize(document)

                file.write(
                    json.dumps(clean_document, ensure_ascii=False) + "\n"
                )

                total += 1

                if total % 50_000 == 0:
                    print(f"Exported {total:,} documents...")

        finally:
            cursor.close()

    print(f"Completed: {total:,} documents")
    print(f"Saved to: {output_file}")

    return total


# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000
    )

    try:
        client.admin.command("ping")
        print("MongoDB connection successful.")

        database = client[DATABASE_NAME]

        available = database.list_collection_names()

        print(f"\nDatabase: {DATABASE_NAME}")
        print("Available collections:", available)

        total_documents = 0

        for collection_name in COLLECTIONS:

            if collection_name not in available:
                raise ValueError(
                    f"Required collection not found: {collection_name}"
                )

            total_documents += export_collection(
                database,
                collection_name
            )

        print("\nMONGODB EXPORT COMPLETED SUCCESSFULLY.")
        print(f"Total exported documents: {total_documents:,}")
        print(f"Output directory: {OUTPUT_DIR}")

    finally:
        client.close()


if __name__ == "__main__":
    main()