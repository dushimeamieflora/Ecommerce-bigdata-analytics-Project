import json

report_path = "results/data_quality/sessions_quality.json"

with open(report_path, "r", encoding="utf-8") as file:
    report = json.load(file)

fields = {}

for batch in report["batch_results"]:
    for field, count in batch["missing_top_level_values"].items():
        fields[field] = fields.get(field, 0) + count

print("TOTAL INPUT:", report["total_input_rows"])
print("TOTAL CLEANED:", report["total_cleaned_rows"])
print("TOTAL REMOVED:", report["total_removed_session_rows"])

print("\nMISSING TOP-LEVEL VALUES:")

for field, count in fields.items():
    print(field + ": " + str(count))