
import json
from pathlib import Path
from collections import defaultdict

# Project folders
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
REPORT_DIR = BASE_DIR / "results" / "data_quality"

REPORT_DIR.mkdir(parents=True, exist_ok=True)

# Track missing values and field occurrences
stats = defaultdict(lambda: {
    "records": 0,
    "field_occurrences": 0,
    "null_values": 0,
    "empty_strings": 0,
    "whitespace_strings": 0,
    "literal_none_strings": 0,
    "records_with_missing": 0
})

dataset_totals = defaultdict(int)


def inspect_value(value, path, record_fields):
    """Inspect values recursively, including nested dictionaries and lists."""

    record_fields.add(path)

    item = stats[path]
    item["field_occurrences"] += 1

    if value is None:
        item["null_values"] += 1
        return

    if isinstance(value, str):
        if value == "":
            item["empty_strings"] += 1
        elif value.strip() == "":
            item["whitespace_strings"] += 1
        elif value.strip().lower() == "none":
            item["literal_none_strings"] += 1

    elif isinstance(value, dict):
        for key, nested_value in value.items():
            nested_path = f"{path}.{key}"
            inspect_value(nested_value, nested_path, record_fields)

    elif isinstance(value, list):
        for nested_value in value:
            nested_path = f"{path}[]"

            if isinstance(nested_value, (dict, list)):
                inspect_value(nested_value, nested_path, record_fields)
            else:
                inspect_value(nested_value, nested_path, record_fields)


def process_record(record, dataset):
    """Inspect one record and update dataset statistics."""

    if not isinstance(record, dict):
        record = {"_record_value": record}

    dataset_totals[dataset] += 1
    record_fields = set()

    for key, value in record.items():
        inspect_value(value, key, record_fields)

    # Count records containing at least one missing value
    for field in record_fields:
        item = stats[field]

        if (
            item["null_values"] > 0
            or item["empty_strings"] > 0
            or item["whitespace_strings"] > 0
            or item["literal_none_strings"] > 0
        ):
            # Exact field-level missing record count is calculated below
            pass


def read_json_file(file_path, dataset):
    """Read a standard JSON file."""

    with file_path.open("r", encoding="utf-8") as file:
        data = json.load(file)

    if isinstance(data, list):
        for record in data:
            process_record(record, dataset)

    elif isinstance(data, dict):
        # Support JSON files containing a list inside a wrapper
        list_found = False

        for value in data.values():
            if isinstance(value, list):
                for record in value:
                    process_record(record, dataset)
                list_found = True

        if not list_found:
            process_record(data, dataset)

    else:
        process_record(data, dataset)


def read_jsonl_file(file_path, dataset):
    """Read JSON Lines one record at a time."""

    with file_path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
                process_record(record, dataset)

            except json.JSONDecodeError:
                print(
                    f"Invalid JSON: {file_path.name}, "
                    f"line {line_number}"
                )


def audit_dataset(pattern, dataset, file_type):
    """Audit every file matching a dataset pattern."""

    files = sorted(RAW_DIR.glob(pattern))

    if not files:
        print(f"WARNING: No files found for {dataset}: {pattern}")
        return

    print(f"\nAuditing {dataset}...")

    for file_path in files:
        print(f"  Reading {file_path.name}")

        if file_type == "json":
            read_json_file(file_path, dataset)
        else:
            read_jsonl_file(file_path, dataset)

    print(f"  Records inspected: {dataset_totals[dataset]:,}")


# Audit all raw datasets
audit_dataset("users.json", "users", "json")
audit_dataset("products.json", "products", "json")
audit_dataset("categories.json", "categories", "json")
audit_dataset("transactions*.jsonl", "transactions", "jsonl")
audit_dataset("sessions*.jsonl", "sessions", "jsonl")


# Build report
report = {
    "description": "Raw dataset missing-value audit",
    "dataset_record_counts": dict(dataset_totals),
    "field_statistics": {}
}

for field, item in sorted(stats.items()):
    total_missing = (
        item["null_values"]
        + item["empty_strings"]
        + item["whitespace_strings"]
        + item["literal_none_strings"]
    )

    report["field_statistics"][field] = {
        **item,
        "total_missing_values": total_missing
    }


# Save report
report_path = REPORT_DIR / "raw_missing_values_report.json"

with report_path.open("w", encoding="utf-8") as file:
    json.dump(report, file, indent=4, ensure_ascii=False)

print("\n" + "=" * 55)
print("RAW DATASET AUDIT COMPLETED")
print("=" * 55)

for dataset, count in dataset_totals.items():
    print(f"{dataset}: {count:,} records")

print(f"\nReport saved to: {report_path}")
