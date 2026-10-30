
import csv
import json
import subprocess
import sys
from pathlib import Path
from statistics import median

INPUT = "data/benchmark_1m/events.csv"
DIMENSION = "data/benchmark_1m/customers.csv"

TRIALS = 3
SALT_BUCKETS = [8, 16, 32]

OUTPUT_DIR = Path("outputs/repeated_trials")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

results = []

for buckets in SALT_BUCKETS:
    for trial in range(1, TRIALS + 1):
        run_name = f"salt{buckets}_trial{trial}"
        output_path = OUTPUT_DIR / run_name

        print(f"\nRunning {run_name}...")

        command = [
            sys.executable,
            "-m", "src.main",
            "--input", INPUT,
            "--dimension", DIMENSION,
            "--output", str(output_path),
            "--salt-buckets", str(buckets),
            "--shuffle-partitions", "8",
        ]

        subprocess.run(command, check=True)

        metrics_path = output_path / "run_metrics.json"

        with open(metrics_path, "r") as file:
            metrics = json.load(file)

        if metrics["status"] != "success":
            raise RuntimeError(f"{run_name} did not succeed")

        if not metrics["comparison"]["matches"]:
            raise RuntimeError(f"{run_name} failed correctness")

        if not metrics["accepted_plus_rejected_equals_input"]:
            raise RuntimeError(f"{run_name} failed reconciliation")

        results.append({
            "salt_buckets": buckets,
            "trial": trial,
            "baseline_seconds": metrics["baseline_seconds"],
            "salted_seconds": metrics["salted_seconds"],
            "correctness": metrics["comparison"]["matches"],
            "reconciliation": metrics["accepted_plus_rejected_equals_input"],
        })

        print(f"Completed {run_name}")

# Save individual trial results
trial_file = OUTPUT_DIR / "trial_results.csv"

with open(trial_file, "w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=results[0].keys())
    writer.writeheader()
    writer.writerows(results)

# Calculate median runtimes
summary = []

for buckets in SALT_BUCKETS:
    group = [
        row for row in results
        if row["salt_buckets"] == buckets
    ]

    summary.append({
        "salt_buckets": buckets,
        "trials": len(group),
        "median_baseline_seconds": round(
            median(row["baseline_seconds"] for row in group), 4
        ),
        "median_salted_seconds": round(
            median(row["salted_seconds"] for row in group), 4
        ),
        "all_correct": all(row["correctness"] for row in group),
        "all_reconciled": all(row["reconciliation"] for row in group),
    })

summary_file = OUTPUT_DIR / "median_summary.csv"

with open(summary_file, "w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=summary[0].keys())
    writer.writeheader()
    writer.writerows(summary)

print("\n========== MEDIAN BENCHMARK SUMMARY ==========")

for row in summary:
    print(row)

print(f"\nTrial results: {trial_file}")
print(f"Median summary: {summary_file}")
