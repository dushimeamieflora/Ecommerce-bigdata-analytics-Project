
import os
import json
from collections import defaultdict

# --------------------------------------------------
# 1. PATHS
# --------------------------------------------------

PROJECT_DIR = os.getcwd()

CHUNKS_DIR = os.path.join(
    PROJECT_DIR, "data", "temp", "sessions"
)

OUTPUT_DIR = os.path.join(
    PROJECT_DIR, "results", "data_quality"
)

OUTPUT_FILE = os.path.join(
    OUTPUT_DIR, "nested_sessions_quality.json"
)

os.makedirs(OUTPUT_DIR, exist_ok=True)

# --------------------------------------------------
# 2. COUNTERS
# --------------------------------------------------

total_sessions = 0
total_page_views = 0
empty_page_view_sessions = 0

page_type_stats = defaultdict(lambda: {
    "total_page_views": 0,
    "missing_product_id": 0,
    "missing_category_id": 0,
    "missing_timestamp": 0,
    "missing_page_type": 0,
    "missing_view_duration": 0
})

# --------------------------------------------------
# 3. FIND CHUNKS
# --------------------------------------------------

chunk_files = sorted([
    file for file in os.listdir(CHUNKS_DIR)
    if file.endswith(".jsonl")
])

print("Number of chunks found:", len(chunk_files))

# --------------------------------------------------
# 4. PROCESS ONE RECORD AT A TIME
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

            page_views = session.get("page_views")

            if not isinstance(page_views, list) or len(page_views) == 0:
                empty_page_view_sessions += 1
                continue

            for view in page_views:

                if not isinstance(view, dict):
                    continue

                total_page_views += 1

                page_type = view.get("page_type")

                if not isinstance(page_type, str) or not page_type.strip():
                    page_type = "UNKNOWN"

                stats = page_type_stats[page_type]

                stats["total_page_views"] += 1

                product_id = view.get("product_id")
                category_id = view.get("category_id")
                timestamp = view.get("timestamp")
                view_duration = view.get("view_duration")

                if product_id is None or (
                    isinstance(product_id, str) and not product_id.strip()
                ):
                    stats["missing_product_id"] += 1

                if category_id is None or (
                    isinstance(category_id, str) and not category_id.strip()
                ):
                    stats["missing_category_id"] += 1

                if timestamp is None:
                    stats["missing_timestamp"] += 1

                if not isinstance(
                    view.get("page_type"), str
                ) or not view.get("page_type", "").strip():
                    stats["missing_page_type"] += 1

                if view_duration is None:
                    stats["missing_view_duration"] += 1

# --------------------------------------------------
# 5. CREATE REPORT
# --------------------------------------------------

report = {
    "total_sessions_processed": total_sessions,
    "total_page_views": total_page_views,
    "sessions_with_missing_or_empty_page_views":
        empty_page_view_sessions,
    "page_view_quality_by_page_type": dict(page_type_stats)
}

# --------------------------------------------------
# 6. SAVE REPORT
# --------------------------------------------------

with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
    json.dump(report, file, indent=4)

# --------------------------------------------------
# 7. DISPLAY RESULTS
# --------------------------------------------------

print("\n======================================")
print("NESTED SESSION QUALITY CHECK COMPLETE")
print("======================================")

print("Total sessions:", f"{total_sessions:,}")
print("Total page views:", f"{total_page_views:,}")

print(
    "Sessions with missing/empty page_views:",
    f"{empty_page_view_sessions:,}"
)

print("\nQUALITY BY PAGE TYPE:")

for page_type, stats in sorted(page_type_stats.items()):

    print("\nPage type:", page_type)

    for field, value in stats.items():
        print(f"  {field}: {value:,}")

print("\nReport saved to:")
print(OUTPUT_FILE)
