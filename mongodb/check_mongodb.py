
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient(
    "mongodb://localhost:27017",
    serverSelectionTimeoutMS=5000
)

try:
    # Verify MongoDB connection
    client.admin.command("ping")

    # Select project database
    db = client["ecommerce_analytics"]

    print("=" * 58)
    print("           AUCA E-COMMERCE BIG DATA PROJECT")
    print("              MONGODB DATABASE VERIFICATION")
    print("=" * 58)

    print("\nConnection Status : SUCCESS")
    print(f"Database          : {db.name}")

    print("\nMONGODB COLLECTIONS AND DOCUMENT COUNTS")
    print("-" * 58)
    print(f"{'Collection':<25} {'Documents':>15}")
    print("-" * 58)

    total_documents = 0
    collection_names = sorted(db.list_collection_names())

    for collection_name in collection_names:
        count = db[collection_name].count_documents({})
        total_documents += count

        print(f"{collection_name:<25} {count:>15,}")

    print("-" * 58)
    print(f"{'TOTAL DOCUMENTS':<25} {total_documents:>15,}")
    print(f"{'TOTAL COLLECTIONS':<25} {len(collection_names):>15}")
    print("=" * 58)

    print("\nMongoDB verification completed successfully!")

except Exception as error:
    print(f"\nMongoDB verification FAILED: {error}")
    raise

finally:
    client.close()

