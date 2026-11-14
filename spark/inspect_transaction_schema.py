
import os
import json
import glob

pattern = os.path.join(
    "data", "raw", "transactions_*.jsonl"
)

files = sorted(glob.glob(pattern))

if not files:
    raise FileNotFoundError(
        "No transaction JSONL files found."
    )

with open(files[0], "r", encoding="utf-8") as file:
    for line in file:
        if line.strip():
            transaction = json.loads(line)
            break

print("TRANSACTION FIELDS:")
print(list(transaction.keys()))

print("\nSAMPLE TRANSACTION:")
print(json.dumps(transaction, indent=4))

if isinstance(transaction.get("items"), list) and transaction["items"]:
    print("\nFIRST TRANSACTION ITEM:")
    print(json.dumps(transaction["items"][0], indent=4))
else:
    print("\nNo transaction items found in this sample.")
