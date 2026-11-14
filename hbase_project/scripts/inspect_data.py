
import json
from pathlib import Path

data_folder = Path("data/raw")

samples = {
    "products.json": ["price_history"],
    "categories.json": ["subcategories"],
    "users.json": ["geo_data"],
    "sessions_001.jsonl": [
        "geo_data", "device_profile", "viewed_products",
        "page_views", "cart_contents"
    ],
    "transactions_001.jsonl": ["items"]
}

for filename, fields in samples.items():
    file_path = data_folder / filename

    print(f"\n--- {filename} ---")

    with open(file_path, "r", encoding="utf-8") as file:
        if filename.endswith(".jsonl"):
            record = json.loads(file.readline())
        else:
            data = json.load(file)
            record = data[0] if isinstance(data, list) else data

    for field in fields:
        value = record.get(field)
        print(f"\n{field}:")
        print(json.dumps(value, indent=2, ensure_ascii=False)[:1200])

