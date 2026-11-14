
from pymongo import MongoClient
from pprint import pprint

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017")

db = client["ecommerce_analytics"]

# Retrieve one transaction
transaction = db["transactions"].find_one()

print("\nMONGODB TRANSACTION DOCUMENT")
print("=" * 50)

pprint(transaction)

print("\nTRANSACTION FIELD NAMES")
print("=" * 50)

if transaction:
    for field, value in transaction.items():
        print(f"{field}: {type(value).__name__}")

client.close()
