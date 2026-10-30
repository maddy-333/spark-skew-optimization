import json
import csv
from pathlib import Path

runs = {
    "8_buckets": "outputs/run_1m_salt8/run_metrics.json",
    "16_buckets": "outputs/run_1m/run_metrics.json",
    "32_buckets": "outputs/run_1m_salt32/run_metrics.json",
}

results = []

for configuration, file_path in runs.items():
    with open(file_path, "r") as file:
        data = json.load(file)

    results.append({
        "configuration": configuration,
        "input_rows": data["input_rows"],
        "accepted_rows": data["accepted_rows"],
        "rejected_rows": data["rejected_rows"],
        "baseline_seconds": data["baseline_seconds"],
        "salted_seconds": data["salted_seconds"],
        "correctness": data["comparison"]["matches"],
        "reconciliation": data["accepted_plus_rejected_equals_input"],
    })

output_file = Path("outputs/salt_bucket_comparison.csv")

with open(output_file, "w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=results[0].keys())
    writer.writeheader()
    writer.writerows(results)

print("\nBenchmark comparison:")
for result in results:
    print(result)

print(f"\nReport saved to: {output_file}")
