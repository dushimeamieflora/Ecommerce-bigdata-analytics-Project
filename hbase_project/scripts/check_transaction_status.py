
import json
from pathlib import Path
from collections import Counter

data_folder = Path("data/raw")

statuses = Counter()
timestamp_examples = []
records_checked = 0

files = sorted(data_folder.glob("transactions_*.jsonl"))

for file_path in files:
    if file_path.stat().st_size == 0:
        continue

    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue

            record = json.loads(line)

            statuses[record.get("status")] += 1
            records_checked += 1

            if len(timestamp_examples) < 5:
                timestamp_examples.append(record.get("timestamp"))

print("Total transactions checked:", records_checked)

print("\nTransaction status counts:")
for status, count in statuses.items():
    print(f"{status}: {count}")

print("\nTimestamp examples:")
for timestamp in timestamp_examples:
    print(timestamp)
