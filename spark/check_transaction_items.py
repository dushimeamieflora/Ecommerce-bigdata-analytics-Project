
import os
import glob
import json
from collections import defaultdict

# --------------------------------------------------
# 1. Load valid product IDs
# --------------------------------------------------

products_file = os.path.join("data", "raw", "products.json")

with open(products_file, "r", encoding="utf-8") as file:
    products_data = json.load(file)

if isinstance(products_data, dict):
    products_data = products_data.get("products", [])

valid_product_ids = {
    product.get("product_id")
    for product in products_data
    if product.get("product_id")
}

print("Valid products loaded:", len(valid_product_ids))

# --------------------------------------------------
# 2. Prepare validation counters
# --------------------------------------------------

stats = defaultdict(int)

stats["invalid_item_subtotals"] = 0
stats["invalid_transaction_subtotals"] = 0
stats["invalid_transaction_totals"] = 0

# --------------------------------------------------
# 3. Read transactions one line at a time
# --------------------------------------------------

pattern = os.path.join("data", "raw", "transactions_*.jsonl")
files = sorted(glob.glob(pattern))

if not files:
    raise FileNotFoundError("No transaction JSONL files found.")

for file_path in files:
    print("Checking:", os.path.basename(file_path))

    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue

            transaction = json.loads(line)
            stats["total_transactions"] += 1

            items = transaction.get("items")

            if not isinstance(items, list) or not items:
                stats["transactions_without_items"] += 1
                continue

            calculated_transaction_subtotal = 0

            for item in items:
                stats["total_items"] += 1

                if not isinstance(item, dict):
                    stats["malformed_items"] += 1
                    continue

                product_id = item.get("product_id")
                quantity = item.get("quantity")
                unit_price = item.get("unit_price")
                item_subtotal = item.get("subtotal")

                if not product_id:
                    stats["missing_product_ids"] += 1
                elif product_id not in valid_product_ids:
                    stats["unknown_product_ids"] += 1

                if quantity is None:
                    stats["missing_quantities"] += 1
                elif not isinstance(quantity, (int, float)) or quantity <= 0:
                    stats["invalid_quantities"] += 1

                if unit_price is None:
                    stats["missing_unit_prices"] += 1
                elif not isinstance(unit_price, (int, float)) or unit_price < 0:
                    stats["invalid_unit_prices"] += 1

                if item_subtotal is None:
                    stats["missing_item_subtotals"] += 1
                elif not isinstance(item_subtotal, (int, float)) or item_subtotal < 0:
                    stats["invalid_item_subtotals"] += 1

                if (
                    isinstance(quantity, (int, float))
                    and quantity > 0
                    and isinstance(unit_price, (int, float))
                    and unit_price >= 0
                ):
                    calculated = quantity * unit_price
                    calculated_transaction_subtotal += calculated

                    if (
                        isinstance(item_subtotal, (int, float))
                        and round(calculated, 2) != round(item_subtotal, 2)
                    ):
                        stats["item_subtotal_mismatches"] += 1

            # Compare calculated item subtotals with transaction subtotal
            transaction_subtotal = transaction.get("subtotal")

            if (
                isinstance(transaction_subtotal, (int, float))
                and round(calculated_transaction_subtotal, 2)
                != round(transaction_subtotal, 2)
            ):
                stats["transaction_subtotal_mismatches"] += 1

            # Check total = subtotal - discount
            discount = transaction.get("discount")
            total = transaction.get("total")

            if (
                isinstance(transaction_subtotal, (int, float))
                and isinstance(discount, (int, float))
                and isinstance(total, (int, float))
            ):
                expected_total = transaction_subtotal - discount

                if round(expected_total, 2) != round(total, 2):
                    stats["transaction_total_mismatches"] += 1

# --------------------------------------------------
# 4. Display results
# --------------------------------------------------

print("\n========== TRANSACTION ITEMS QUALITY REPORT ==========")

for key, value in stats.items():
    print(f"{key}: {value}")

# --------------------------------------------------
# 5. Save report
# --------------------------------------------------

report = {
    "dataset": "transaction_items",
    "valid_product_count": len(valid_product_ids),
    "validation_results": dict(stats)
}

output_folder = os.path.join("results", "data_quality")
os.makedirs(output_folder, exist_ok=True)

output_file = os.path.join(
    output_folder,
    "transaction_items_quality.json"
)

with open(output_file, "w", encoding="utf-8") as file:
    json.dump(report, file, indent=4)

print("\nReport saved to:", output_file)

