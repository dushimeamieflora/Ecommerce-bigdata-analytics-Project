
import json
import math
from pathlib import Path
from collections import Counter
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
REPORT_DIR = BASE_DIR / "results" / "data_quality"

REPORT_DIR.mkdir(parents=True, exist_ok=True)

report = {}


def is_valid_number(value):
    """Check that a value is a finite number, excluding Boolean values."""
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def is_valid_date(value):
    """Check whether a date string can be parsed."""
    if not isinstance(value, str) or not value.strip():
        return False

    try:
        datetime.fromisoformat(value)
        return True
    except ValueError:
        return False


def load_json_records(file_path):
    with file_path.open("r", encoding="utf-8") as file:
        data = json.load(file)

    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for value in data.values():
            if isinstance(value, list):
                return value
        return [data]

    return [data]


def inspect_dataset(name, records, id_field, required_fields):
    print(f"\nValidating {name}...")

    ids = []
    exact_records = Counter()
    missing_fields = Counter()
    invalid_values = Counter()

    for record in records:
        if not isinstance(record, dict):
            invalid_values["record_not_object"] += 1
            continue

        # Check IDs
        record_id = record.get(id_field)

        if record_id is None or (
            isinstance(record_id, str) and not record_id.strip()
        ):
            missing_fields[id_field] += 1
        else:
            ids.append(str(record_id))

        # Check exact duplicate records
        serialized = json.dumps(
            record, sort_keys=True, ensure_ascii=False
        )
        exact_records[serialized] += 1

        # Check required top-level fields
        for field in required_fields:
            value = record.get(field)

            if value is None or (
                isinstance(value, str) and not value.strip()
            ):
                missing_fields[field] += 1

        # Product validation
        if name == "products":
            base_price = record.get("base_price")
            stock = record.get("current_stock")

            if not is_valid_number(base_price):
                invalid_values["invalid_base_price_type"] += 1
            elif base_price < 0:
                invalid_values["negative_base_price"] += 1

            if not is_valid_number(stock):
                invalid_values["invalid_stock_type"] += 1
            elif stock < 0:
                invalid_values["negative_stock"] += 1

            if not isinstance(record.get("is_active"), bool):
                invalid_values["invalid_is_active_type"] += 1

            if not is_valid_date(record.get("creation_date")):
                invalid_values["invalid_creation_date"] += 1

            # Validate nested price history
            price_history = record.get("price_history")

            if not isinstance(price_history, list) or len(price_history) == 0:
                invalid_values["missing_or_empty_price_history"] += 1
            else:
                for entry in price_history:
                    if not isinstance(entry, dict):
                        invalid_values["invalid_price_history_structure"] += 1
                        continue

                    historical_price = entry.get("price")
                    historical_date = entry.get("date")

                    if historical_price is None:
                        missing_fields["price_history[].price"] += 1
                    elif not is_valid_number(historical_price):
                        invalid_values["invalid_historical_price_type"] += 1
                    elif historical_price < 0:
                        invalid_values["negative_historical_price"] += 1

                    if historical_date is None or (
                        isinstance(historical_date, str)
                        and not historical_date.strip()
                    ):
                        missing_fields["price_history[].date"] += 1
                    elif not is_valid_date(historical_date):
                        invalid_values["invalid_price_history_date"] += 1

        # Transaction validation
        elif name == "transactions":
            total = record.get("total")
            subtotal = record.get("subtotal")
            timestamp = record.get("timestamp")

            if not is_valid_number(total):
                invalid_values["invalid_total_type"] += 1
            elif total < 0:
                invalid_values["negative_total"] += 1

            if not is_valid_number(subtotal):
                invalid_values["invalid_subtotal_type"] += 1
            elif subtotal < 0:
                invalid_values["negative_subtotal"] += 1

            if not is_valid_date(timestamp):
                invalid_values["invalid_timestamp"] += 1

            items = record.get("items")

            if not isinstance(items, list) or len(items) == 0:
                invalid_values["missing_or_empty_items"] += 1
            else:
                for item in items:
                    if not isinstance(item, dict):
                        invalid_values["invalid_item_structure"] += 1
                        continue

                    for field in [
                        "product_id", "quantity", "unit_price", "subtotal"
                    ]:
                        value = item.get(field)

                        if value is None or (
                            isinstance(value, str) and not value.strip()
                        ):
                            missing_fields[f"items[].{field}"] += 1

                    quantity = item.get("quantity")
                    unit_price = item.get("unit_price")
                    item_subtotal = item.get("subtotal")

                    if not is_valid_number(quantity):
                        invalid_values["invalid_item_quantity_type"] += 1
                    elif quantity <= 0:
                        invalid_values["nonpositive_item_quantity"] += 1

                    if not is_valid_number(unit_price):
                        invalid_values["invalid_item_unit_price_type"] += 1
                    elif unit_price < 0:
                        invalid_values["negative_item_unit_price"] += 1

                    if not is_valid_number(item_subtotal):
                        invalid_values["invalid_item_subtotal_type"] += 1
                    elif item_subtotal < 0:
                        invalid_values["negative_item_subtotal"] += 1

    # Duplicate ID summary
    id_counts = Counter(ids)

    duplicate_ids = {
        key: count for key, count in id_counts.items() if count > 1
    }

    exact_duplicate_count = sum(
        count - 1 for count in exact_records.values() if count > 1
    )

    result = {
        "record_count": len(records),
        "unique_id_count": len(id_counts),
        "duplicate_id_count": sum(
            count - 1 for count in duplicate_ids.values()
        ),
        "duplicate_id_examples": dict(list(duplicate_ids.items())[:10]),
        "exact_duplicate_record_count": exact_duplicate_count,
        "missing_field_counts": dict(missing_fields),
        "invalid_value_counts": dict(invalid_values)
    }

    report[name] = result

    print(f"Records: {len(records):,}")
    print(f"Unique IDs: {len(id_counts):,}")
    print(f"Duplicate IDs: {result['duplicate_id_count']:,}")
    print(f"Exact duplicate records: {exact_duplicate_count:,}")
    print(f"Missing fields: {sum(missing_fields.values()):,}")
    print(f"Invalid values: {sum(invalid_values.values()):,}")


# 1. Users
users = load_json_records(RAW_DIR / "users.json")

inspect_dataset(
    "users",
    users,
    "user_id",
    [
        "user_id", "age", "gender",
        "registration_date", "last_active"
    ]
)


# 2. Products
products = load_json_records(RAW_DIR / "products.json")

inspect_dataset(
    "products",
    products,
    "product_id",
    [
        "product_id", "name", "category_id",
        "subcategory_id", "base_price", "current_stock",
        "is_active", "price_history", "creation_date"
    ]
)


# 3. Categories
categories = load_json_records(RAW_DIR / "categories.json")

inspect_dataset(
    "categories",
    categories,
    "category_id",
    ["category_id", "name", "subcategories"]
)


# 4. Transactions: read JSONL files
transactions = []
transaction_files = sorted(RAW_DIR.glob("transactions*.jsonl"))

json_errors = 0

for file_path in transaction_files:
    with file_path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue

            try:
                transactions.append(json.loads(line))
            except json.JSONDecodeError:
                json_errors += 1
                print(
                    f"Invalid JSON: {file_path.name}, "
                    f"line {line_number}"
                )

inspect_dataset(
    "transactions",
    transactions,
    "transaction_id",
    [
        "transaction_id", "user_id", "timestamp",
        "items", "subtotal", "discount", "total",
        "payment_method", "status"
    ]
)

report["transactions"]["invalid_json_lines"] = json_errors


# 5. Save validation report
output_path = REPORT_DIR / "core_datasets_validation.json"

with output_path.open("w", encoding="utf-8") as file:
    json.dump(report, file, indent=4, ensure_ascii=False)


print("\n" + "=" * 55)
print("CORE DATASET VALIDATION COMPLETED")
print("=" * 55)
print(f"Report saved to: {output_path}")
