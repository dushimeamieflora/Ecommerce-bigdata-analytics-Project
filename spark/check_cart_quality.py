
import os
import json
from collections import defaultdict

# --------------------------------------------------
# 1. PROJECT PATHS
# --------------------------------------------------

PROJECT_DIR = os.getcwd()

CHUNKS_DIR = os.path.join(
    PROJECT_DIR, "data", "temp", "sessions"
)

PRODUCTS_FILE = os.path.join(
    PROJECT_DIR, "data", "raw", "products.json"
)

OUTPUT_DIR = os.path.join(
    PROJECT_DIR, "results", "data_quality"
)

OUTPUT_FILE = os.path.join(
    OUTPUT_DIR, "cart_contents_quality.json"
)

os.makedirs(OUTPUT_DIR, exist_ok=True)

# --------------------------------------------------
# 2. LOAD VALID PRODUCT IDS
# --------------------------------------------------

print("Loading product IDs...")

try:
    with open(PRODUCTS_FILE, "r", encoding="utf-8") as file:
        products_data = json.load(file)

except json.JSONDecodeError:
    # Support JSONL files if products.json is line-delimited
    products_data = []

    with open(PRODUCTS_FILE, "r", encoding="utf-8") as file:
        for line in file:
            if line.strip():
                products_data.append(json.loads(line))

if isinstance(products_data, dict):
    if isinstance(products_data.get("products"), list):
        products_data = products_data["products"]
    else:
        products_data = [products_data]

valid_product_ids = {
    str(product["product_id"]).strip()
    for product in products_data
    if isinstance(product, dict)
    and product.get("product_id") is not None
    and str(product["product_id"]).strip()
}

print("Valid product IDs loaded:", len(valid_product_ids))

# --------------------------------------------------
# 3. INITIALIZE COUNTERS
# --------------------------------------------------

total_sessions = 0
sessions_with_empty_carts = 0
sessions_with_carts = 0

total_cart_items = 0
missing_product_ids = 0
unknown_product_ids = 0

missing_quantities = 0
invalid_quantities = 0

missing_prices = 0
invalid_prices = 0

malformed_cart_items = 0

# --------------------------------------------------
# 4. FIND SESSION CHUNKS
# --------------------------------------------------

chunk_files = sorted([
    filename
    for filename in os.listdir(CHUNKS_DIR)
    if filename.endswith(".jsonl")
])

print("\nNumber of chunks found:", len(chunk_files))

# --------------------------------------------------
# 5. PROCESS ONE SESSION AT A TIME
# --------------------------------------------------

for index, filename in enumerate(chunk_files, start=1):

    file_path = os.path.join(CHUNKS_DIR, filename)

    print(
        f"Processing chunk {index}/{len(chunk_files)}: "
        f"{filename}"
    )

    with open(file_path, "r", encoding="utf-8") as file:

        for line in file:

            if not line.strip():
                continue

            session = json.loads(line)
            total_sessions += 1

            cart = session.get("cart_contents")

            if not isinstance(cart, dict) or len(cart) == 0:
                sessions_with_empty_carts += 1
                continue

            sessions_with_carts += 1

            # Each cart key should be a product ID
            for product_id, item in cart.items():

                total_cart_items += 1

                product_id = str(product_id).strip()

                if not product_id:
                    missing_product_ids += 1

                elif product_id not in valid_product_ids:
                    unknown_product_ids += 1

                if not isinstance(item, dict):
                    malformed_cart_items += 1
                    continue

                # Check quantity
                quantity = item.get("quantity")

                if quantity is None:
                    missing_quantities += 1

                else:
                    try:
                        quantity_value = float(quantity)

                        if quantity_value <= 0:
                            invalid_quantities += 1

                    except (ValueError, TypeError):
                        invalid_quantities += 1

                # Check price
                price = item.get("price")

                if price is None:
                    missing_prices += 1

                else:
                    try:
                        price_value = float(price)

                        if price_value < 0:
                            invalid_prices += 1

                    except (ValueError, TypeError):
                        invalid_prices += 1

# --------------------------------------------------
# 6. CREATE QUALITY REPORT
# --------------------------------------------------

report = {
    "total_sessions_processed": total_sessions,
    "sessions_with_carts": sessions_with_carts,
    "sessions_with_empty_or_missing_carts":
        sessions_with_empty_carts,
    "total_cart_items": total_cart_items,
    "valid_product_ids_in_products_dataset":
        len(valid_product_ids),
    "missing_product_ids": missing_product_ids,
    "unknown_product_ids": unknown_product_ids,
    "missing_quantities": missing_quantities,
    "invalid_quantities": invalid_quantities,
    "missing_prices": missing_prices,
    "invalid_prices": invalid_prices,
    "malformed_cart_items": malformed_cart_items
}

# --------------------------------------------------
# 7. SAVE REPORT
# --------------------------------------------------

with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
    json.dump(report, file, indent=4)

# --------------------------------------------------
# 8. DISPLAY RESULTS
# --------------------------------------------------

print("\n======================================")
print("CART CONTENTS QUALITY CHECK COMPLETE")
print("======================================")

for field, value in report.items():
    print(f"{field}: {value:,}")

print("\nReport saved to:")
print(OUTPUT_FILE)
