
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Select database
db = client["ecommerce_analytics"]

print("Creating MongoDB indexes...\n")

# PRODUCTS COLLECTION
db.products.create_index("product_id")
print("Products: product_id index created.")

# CATEGORIES COLLECTION
db.categories.create_index("category_id")
print("Categories: category_id index created.")

# TRANSACTIONS COLLECTION
db.transactions.create_index("transaction_id", unique=True)
db.transactions.create_index("user_id")
db.transactions.create_index("timestamp")
db.transactions.create_index("items.product_id")

print("Transactions: all four indexes created.")

# Verify indexes
print("\nVERIFYING MONGODB INDEXES")

for collection_name in ["products", "categories", "transactions"]:
    print(f"\n{collection_name.upper()}:")

    indexes = db[collection_name].index_information()

    for index_name, details in indexes.items():
        print(f"  {index_name}: {details['key']}")

print("\nMongoDB index verification completed.")

client.close()
