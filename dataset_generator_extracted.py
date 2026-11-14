
import json
import random
import sys
from pathlib import Path
from datetime import datetime, timedelta

from faker import Faker

# --------------------------------------------------
# 1. CONFIGURATION
# --------------------------------------------------

fake = Faker()
random.seed(42)
Faker.seed(42)

TEST_MODE = "--test" in sys.argv

if TEST_MODE:
    NUM_USERS = 100
    NUM_PRODUCTS = 50
    NUM_CATEGORIES = 5
    NUM_SESSIONS = 1_000
    NUM_TRANSACTIONS = 250
    CHUNK_SIZE = 250
else:
    NUM_USERS = 10_000
    NUM_PRODUCTS = 5_000
    NUM_CATEGORIES = 25
    NUM_SESSIONS = 2_000_000
    NUM_TRANSACTIONS = 500_000
    CHUNK_SIZE = 100_000

OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Every fourth session will produce one transaction.
assert NUM_SESSIONS // 4 == NUM_TRANSACTIONS
assert NUM_SESSIONS % 4 == 0

# Remove previous generated JSON and JSONL files.
for old_file in OUTPUT_DIR.glob("*.json"):
    old_file.unlink()

for old_file in OUTPUT_DIR.glob("*.jsonl"):
    old_file.unlink()


# --------------------------------------------------
# 2. HELPER FUNCTIONS
# --------------------------------------------------

def save_json(data, filename):
    path = OUTPUT_DIR / filename
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, default=str)


def open_chunk(prefix, chunk_number):
    path = OUTPUT_DIR / f"{prefix}_{chunk_number:03d}.jsonl"
    return open(path, "w", encoding="utf-8")


def save_jsonl(record, file_handle):
    file_handle.write(json.dumps(record, default=str) + "\n")


def next_chunk(file_handle, count, chunk_number, prefix):
    if count == CHUNK_SIZE:
        file_handle.close()
        chunk_number += 1
        file_handle = open_chunk(prefix, chunk_number)
        count = 0

    return file_handle, count, chunk_number


# --------------------------------------------------
# 3. GENERATE CATEGORIES AND SUBCATEGORIES
# --------------------------------------------------

print("Generating categories...")

categories = []

for i in range(NUM_CATEGORIES):
    category_id = f"cat_{i + 1:03d}"

    subcategories = []

    for j in range(4):
        subcategories.append({
            "subcategory_id": f"sub_{i + 1:03d}_{j:02d}",
            "name": fake.catch_phrase(),
            "profit_margin": round(random.uniform(0.10, 0.40), 2)
        })

    categories.append({
        "category_id": category_id,
        "name": fake.company(),
        "subcategories": subcategories
    })

category_lookup = {
    category["category_id"]: category["subcategories"]
    for category in categories
}


# --------------------------------------------------
# 4. GENERATE USERS
# --------------------------------------------------

print("Generating users...")

users = []

dataset_start = datetime.now() - timedelta(days=90)

for i in range(NUM_USERS):
    registration_date = dataset_start - timedelta(
        days=random.randint(1, 1000)
    )

    city = fake.city()
    state = fake.state_abbr()
    country = fake.country_code()

    users.append({
        "user_id": f"user_{i + 1:06d}",
        "age": random.randint(18, 75),
        "gender": random.choice(["Female", "Male", "Other"]),
        "geo_data": {
            "city": city,
            "state": state,
            "country": country
        },
        "registration_date": registration_date.isoformat(),
        "last_active": registration_date.isoformat()
    })


# --------------------------------------------------
# 5. GENERATE PRODUCTS
# --------------------------------------------------

print("Generating products...")

products = []

for i in range(NUM_PRODUCTS):
    category = random.choice(categories)
    subcategory = random.choice(
        category["subcategories"]
    )

    is_active = random.random() < 0.95

    base_price = round(random.uniform(5, 1000), 2)

    creation_date = dataset_start - timedelta(
        days=random.randint(1, 365)
    )

    previous_price = round(
        base_price * random.uniform(0.85, 1.15), 2
    )

    products.append({
        "product_id": f"prod_{i + 1:06d}",
        "name": fake.catch_phrase(),
        "category_id": category["category_id"],
        "subcategory_id": subcategory["subcategory_id"],
        "base_price": base_price,
        "current_stock": 10000 if is_active else 0,
        "is_active": is_active,
        "price_history": [
            {
                "price": previous_price,
                "date": (creation_date + timedelta(days=10)).isoformat()
            },
            {
                "price": base_price,
                "date": (creation_date + timedelta(days=30)).isoformat()
            }
        ],
        "creation_date": creation_date.isoformat()
    })

active_products = [
    product for product in products
    if product["is_active"]
]

if not active_products:
    raise RuntimeError("No active products were generated.")

product_lookup = {
    product["product_id"]: product
    for product in products
}


# --------------------------------------------------
# 6. GENERATE SESSIONS AND TRANSACTIONS
# --------------------------------------------------

print("Generating sessions and transactions...")

session_file = open_chunk("sessions", 1)
transaction_file = open_chunk("transactions", 1)

session_chunk = 1
transaction_chunk = 1

sessions_in_chunk = 0
transactions_in_chunk = 0

transaction_count = 0

for i in range(NUM_SESSIONS):

    # Select the user who owns this session.
    user = random.choice(users)

    session_id = f"sess_{i + 1:010d}"

    start_time = dataset_start + timedelta(
        seconds=random.randint(0, 90 * 24 * 60 * 60)
    )

    duration = random.randint(60, 3600)
    end_time = start_time + timedelta(seconds=duration)

    # Update the user's last activity.
    current_last_active = datetime.fromisoformat(
        user["last_active"]
    )

    if end_time > current_last_active:
        user["last_active"] = end_time.isoformat()

    # Select products viewed during the session.
    number_viewed = random.randint(
        1, min(4, len(active_products))
    )

    viewed = random.sample(
        active_products, number_viewed
    )

    viewed_product_ids = [
        product["product_id"] for product in viewed
    ]

    # Exactly one transaction for every fourth session.
    is_converted = (i % 4 == 0)

    cart_contents = {}

    if is_converted:
        cart_products = random.sample(
            viewed,
            random.randint(1, len(viewed))
        )
    elif random.random() < 0.20:
        cart_products = random.sample(
            viewed,
            random.randint(1, len(viewed))
        )
    else:
        cart_products = []

    for product in cart_products:
        quantity = random.randint(1, 3)

        cart_contents[product["product_id"]] = {
            "quantity": quantity,
            "price": product["base_price"]
        }

    # Create chronological page views.
    page_views = []

    step = max(
        1,
        duration // (len(viewed) + 3)
    )

    page_views.append({
        "timestamp": start_time.isoformat(),
        "page_type": "home",
        "product_id": None,
        "category_id": None,
        "view_duration": random.randint(5, 60)
    })

    page_views.append({
        "timestamp": (
            start_time + timedelta(seconds=step)
        ).isoformat(),
        "page_type": "category_listing",
        "product_id": None,
        "category_id": viewed[0]["category_id"],
        "view_duration": random.randint(10, 90)
    })

    for position, product in enumerate(viewed):
        page_views.append({
            "timestamp": (
                start_time + timedelta(
                    seconds=step * (position + 2)
                )
            ).isoformat(),
            "page_type": "product_detail",
            "product_id": product["product_id"],
            "category_id": product["category_id"],
            "view_duration": random.randint(20, 180)
        })

    if cart_contents:
        page_views.append({
            "timestamp": (
                end_time - timedelta(seconds=step)
            ).isoformat(),
            "page_type": "cart",
            "product_id": None,
            "category_id": None,
            "view_duration": random.randint(10, 120)
        })

    if is_converted:
        page_views.append({
            "timestamp": end_time.isoformat(),
            "page_type": "checkout",
            "product_id": None,
            "category_id": None,
            "view_duration": random.randint(20, 180)
        })

    device_type = random.choice([
        "mobile", "desktop", "tablet"
    ])

    if device_type == "mobile":
        operating_system = random.choice([
            "Android", "iOS"
        ])
    elif device_type == "desktop":
        operating_system = random.choice([
            "Windows", "macOS", "Linux"
        ])
    else:
        operating_system = random.choice([
            "Android", "iPadOS"
        ])

    browser = random.choice([
        "Chrome", "Firefox", "Safari", "Edge"
    ])

    session = {
        "session_id": session_id,
        "user_id": user["user_id"],
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": duration,
        "geo_data": {
            **user["geo_data"],
            "ip_address": fake.ipv4()
        },
        "device_profile": {
            "type": device_type,
            "os": operating_system,
            "browser": browser
        },
        "viewed_products": viewed_product_ids,
        "page_views": page_views,
        "cart_contents": cart_contents,
        "conversion_status": (
            "converted" if is_converted
            else "not_converted"
        ),
        "referrer": random.choice([
            "search_engine",
            "social_media",
            "direct",
            "advertisement",
            "email"
        ])
    }

    save_jsonl(session, session_file)
    sessions_in_chunk += 1

    session_file, sessions_in_chunk, session_chunk = next_chunk(
        session_file,
        sessions_in_chunk,
        session_chunk,
        "sessions"
    )

    # Generate one transaction for converted sessions.
    if is_converted:

        items = []
        subtotal = 0

        for product_id, cart_item in cart_contents.items():
            product = product_lookup[product_id]

            quantity = cart_item["quantity"]

            if product["current_stock"] < quantity:
                raise RuntimeError(
                    "Insufficient stock for transaction."
                )

            unit_price = product["base_price"]
            item_subtotal = round(
                unit_price * quantity, 2
            )

            items.append({
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "subtotal": item_subtotal
            })

            subtotal += item_subtotal

            # Reduce inventory after the purchase.
            product["current_stock"] -= quantity

        subtotal = round(subtotal, 2)

        discount = round(
            subtotal * random.uniform(0, 0.10), 2
        )

        total = round(subtotal - discount, 2)

        transaction = {
            "transaction_id": (
                f"txn_{transaction_count + 1:010d}"
            ),
            "session_id": session_id,
            "user_id": user["user_id"],
            "timestamp": end_time.isoformat(),
            "items": items,
            "subtotal": subtotal,
            "discount": discount,
            "total": total,
            "payment_method": random.choice([
                "credit_card",
                "mobile_money",
                "paypal",
                "bank_transfer"
            ]),
            "status": random.choice([
                "completed",
                "completed",
                "shipped",
                "delivered"
            ])
        }

        save_jsonl(transaction, transaction_file)

        transactions_in_chunk += 1
        transaction_count += 1

        transaction_file, transactions_in_chunk, transaction_chunk = next_chunk(
            transaction_file,
            transactions_in_chunk,
            transaction_chunk,
            "transactions"
        )

    if (i + 1) % max(1, NUM_SESSIONS // 10) == 0:
        print(
            f"Sessions generated: {i + 1:,} / "
            f"{NUM_SESSIONS:,}"
        )

session_file.close()
transaction_file.close()


# --------------------------------------------------
# 7. SAVE REFERENCE DATA
# --------------------------------------------------

print("Saving users, products, and categories...")

save_json(users, "users.json")
save_json(products, "products.json")
save_json(categories, "categories.json")


# --------------------------------------------------
# 8. FINAL SUMMARY
# --------------------------------------------------

print("\nDATA GENERATION COMPLETED")

print(f"Mode: {'TEST' if TEST_MODE else 'FULL'}")
print(f"Users: {len(users):,}")
print(f"Products: {len(products):,}")
print(f"Categories: {len(categories):,}")
print(f"Sessions: {NUM_SESSIONS:,}")
print(f"Transactions: {transaction_count:,}")

print(f"Output folder: {OUTPUT_DIR.resolve()}")

if transaction_count != NUM_TRANSACTIONS:
    raise RuntimeError(
        "The generated transaction count is incorrect."
    )

print("All target record counts were achieved.")
