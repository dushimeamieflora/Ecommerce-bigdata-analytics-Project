
import json
import sqlite3
from pathlib import Path
from datetime import datetime
from collections import Counter

DATA = Path("data/raw")
DB_FILE = Path("data/processed/full_validation.sqlite")

DB_FILE.parent.mkdir(parents=True, exist_ok=True)

# Start a fresh temporary validation database.
if DB_FILE.exists():
    DB_FILE.unlink()

db = sqlite3.connect(DB_FILE)
cursor = db.cursor()

cursor.execute("""
CREATE TABLE sessions (
    session_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    start_time TEXT NOT NULL,
    end_time TEXT NOT NULL,
    conversion_status TEXT NOT NULL,
    cart_contents TEXT NOT NULL
)
""")

cursor.execute("""
CREATE TABLE transaction_ids (
    transaction_id TEXT PRIMARY KEY,
    session_id TEXT UNIQUE
)
""")

errors = Counter()
purchased_quantities = Counter()

def load_json(filename):
    with open(DATA / filename, encoding="utf-8") as f:
        return json.load(f)

def load_jsonl(pattern):
    for path in sorted(DATA.glob(pattern)):
        with open(path, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)

def check(condition, error_name):
    if not condition:
        errors[error_name] += 1

print("1. Loading reference datasets...")

users = load_json("users.json")
products = load_json("products.json")
categories = load_json("categories.json")

user_ids = {u["user_id"] for u in users}
product_map = {p["product_id"]: p for p in products}
category_map = {c["category_id"]: c for c in categories}

check(len(users) == 10_000, "Incorrect user count")
check(len(user_ids) == len(users), "Duplicate user IDs")
check(len(products) == 5_000, "Incorrect product count")
check(len(product_map) == len(products), "Duplicate product IDs")
check(len(categories) == 25, "Incorrect category count")

subcategory_map = {}

for category in categories:
    for sub in category["subcategories"]:
        subcategory_map[sub["subcategory_id"]] = category["category_id"]

for product in products:
    check(product["category_id"] in category_map,
          "Invalid product category")
    check(product["subcategory_id"] in subcategory_map,
          "Invalid product subcategory")

    if product["subcategory_id"] in subcategory_map:
        check(
            subcategory_map[product["subcategory_id"]]
            == product["category_id"],
            "Subcategory/category mismatch"
        )

print("2. Validating all sessions...")

session_count = 0
converted_count = 0
batch = []

for session in load_jsonl("sessions_*.jsonl"):
    session_count += 1

    check(session["user_id"] in user_ids,
          "Session references unknown user")

    try:
        start = datetime.fromisoformat(session["start_time"])
        end = datetime.fromisoformat(session["end_time"])

        check(end >= start, "Invalid session time order")

        page_times = [
            datetime.fromisoformat(page["timestamp"])
            for page in session["page_views"]
        ]

        check(
            page_times == sorted(page_times),
            "Page views are not chronological"
        )

        check(
            all(start <= t <= end for t in page_times),
            "Page view outside session time"
        )

    except (ValueError, TypeError, KeyError):
        errors["Invalid session timestamp or page view"] += 1

    viewed = set(session["viewed_products"])
    cart = session["cart_contents"]

    check(set(cart).issubset(viewed),
          "Cart product not viewed in session")

    for product_id in viewed:
        check(product_id in product_map,
              "Unknown viewed product")

    for page in session["page_views"]:
        if page["product_id"] is not None:
            check(page["product_id"] in product_map,
                  "Unknown page-view product")

    if session["conversion_status"] == "converted":
        converted_count += 1

    batch.append((
        session["session_id"],
        session["user_id"],
        session["start_time"],
        session["end_time"],
        session["conversion_status"],
        json.dumps(cart)
    ))

    if len(batch) >= 5000:
        try:
            cursor.executemany(
                "INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?)",
                batch
            )
        except sqlite3.IntegrityError:
            errors["Duplicate session IDs"] += 1

        db.commit()
        batch.clear()

if batch:
    try:
        cursor.executemany(
            "INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?)",
            batch
        )
    except sqlite3.IntegrityError:
        errors["Duplicate session IDs"] += 1

    db.commit()

check(session_count == 2_000_000, "Incorrect session count")

print(f"   Sessions checked: {session_count:,}")
print(f"   Converted sessions: {converted_count:,}")

print("3. Validating all transactions...")

transaction_count = 0

for transaction in load_jsonl("transactions_*.jsonl"):
    transaction_count += 1

    session_id = transaction["session_id"]

    try:
        cursor.execute("""
            SELECT user_id, end_time, conversion_status, cart_contents
            FROM sessions
            WHERE session_id = ?
        """, (session_id,))

        result = cursor.fetchone()

        check(result is not None, "Missing transaction session")

        if result:
            session_user, session_end, status, cart_json = result
            cart = json.loads(cart_json)

            check(
                transaction["user_id"] == session_user,
                "Transaction/session user mismatch"
            )

            check(
                transaction["timestamp"] == session_end,
                "Transaction/session timestamp mismatch"
            )

            check(status == "converted",
                  "Transaction session not marked converted")

            for item in transaction["items"]:
                product_id = item["product_id"]

                check(product_id in product_map,
                      "Unknown transaction product")

                check(product_id in cart,
                      "Purchased product not in session cart")

                if product_id in cart:
                    check(
                        item["quantity"] == cart[product_id]["quantity"],
                        "Transaction/cart quantity mismatch"
                    )

                if product_id in product_map:
                    check(
                        product_map[product_id]["is_active"],
                        "Inactive product purchased"
                    )

                purchased_quantities[product_id] += item["quantity"]

        calculated_subtotal = round(
            sum(item["subtotal"] for item in transaction["items"]), 2
        )

        check(
            abs(calculated_subtotal - transaction["subtotal"]) < 0.01,
            "Transaction subtotal mismatch"
        )

        check(
            abs(
                transaction["total"]
                - round(
                    transaction["subtotal"] - transaction["discount"], 2
                )
            ) < 0.01,
            "Transaction total mismatch"
        )

        cursor.execute(
            "INSERT INTO transaction_ids VALUES (?, ?)",
            (transaction["transaction_id"], session_id)
        )

    except sqlite3.IntegrityError:
        errors["Duplicate transaction ID or session transaction"] += 1

check(transaction_count == 500_000, "Incorrect transaction count")

print(f"   Transactions checked: {transaction_count:,}")

print("4. Validating inventory...")

for product in products:
    expected_stock = (
        10_000 - purchased_quantities[product["product_id"]]
        if product["is_active"] else 0
    )

    check(
        product["current_stock"] == expected_stock,
        "Inventory mismatch"
    )

# Every converted session should have one transaction.
cursor.execute("""
SELECT COUNT(*)
FROM sessions
WHERE conversion_status = 'converted'
""")
converted_in_db = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM transaction_ids")
unique_transaction_sessions = cursor.fetchone()[0]

check(
    converted_in_db == unique_transaction_sessions,
    "Conversion/transaction count mismatch"
)

check(
    converted_in_db == 500_000,
    "Incorrect converted session count"
)

print("\nFULL DATASET VALIDATION REPORT")
print("-" * 45)
print(f"Users: {len(users):,}")
print(f"Products: {len(products):,}")
print(f"Categories: {len(categories):,}")
print(f"Sessions: {session_count:,}")
print(f"Transactions: {transaction_count:,}")
print(f"Converted sessions: {converted_in_db:,}")

print("\nVALIDATION RESULT")

if errors:
    print(f"FAILED: {sum(errors.values()):,} issue(s) found")

    for name, count in errors.items():
        print(f"- {name}: {count:,}")
else:
    print("PASSED: All validation checks succeeded.")

db.close()
print("\nValidation database:", DB_FILE)
