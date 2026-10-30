
# Spark Skew Optimization Pipeline

## 1. Project Overview

This project demonstrates data skew in Apache Spark using a reproducible synthetic dataset.

It compares:
- Baseline aggregation
- Salted aggregation

The objective is to diagnose uneven key distribution, apply salting, validate correctness, and compare execution performance.

## 2. Technology Stack

- Python 3.12
- Apache Spark / PySpark 3.5.9
- Java 17
- Pytest
- CSV

## 3. Project Structure

```text
spark_skew_pipeline/
├── data/
├── outputs/
├── src/
│   ├── generate_data.py
│   ├── pipeline.py
│   ├── main.py
│   ├── diagnose_skew.py
│   ├── compare_benchmarks.py
│   └── run_benchmark_trials.py
├── tests/
├── requirements.txt
└── README.md
```

## 4. Environment Setup

Activate the virtual environment:

```bash
source .venv/bin/activate
```

Install dependencies:

```bash
python -m pip install -r requirements.txt
```

## 5. Generate Data

Generate 100K events:

```bash
python -m src.generate_data \
  --rows 100000 \
  --output data/benchmark \
  --seed 42 \
  --hot-key-fraction 0.8
```

Generate 1M events:

```bash
python -m src.generate_data \
  --rows 1000000 \
  --output data/benchmark_1m \
  --seed 42 \
  --hot-key-fraction 0.8
```

The generator creates a hot customer key that receives approximately 80% of events. It also injects invalid records and a duplicate to test data quality handling.

## 6. Run the Pipeline

```bash
python -m src.main \
  --input data/benchmark_1m/events.csv \
  --dimension data/benchmark_1m/customers.csv \
  --output outputs/my_run \
  --salt-buckets 16 \
  --shuffle-partitions 8
```

Use a new output directory for every run. Existing output directories are protected from accidental overwrites.

## 7. Diagnose Skew

```bash
python -m src.diagnose_skew
```

The diagnostic compares partition distributions before and after salting.

## 8. Run Tests

```bash
python -m pytest -v
```

## 9. Benchmark Findings

Repeated 1M-row benchmark results:

| Salt Buckets | Baseline Median (sec) | Salted Median (sec) |
|---|---:|---:|
| 8 | 1.9592 | 2.4547 |
| 16 | 1.9669 | 2.5116 |
| 32 | 1.7863 | 2.4533 |

All repeated trials passed correctness and reconciliation checks.

Salting improved partition distribution, but it did not improve runtime in this local environment. The extra hashing, dimension replication, and aggregation introduced overhead.

## 10. Data Quality

The pipeline checks:
- Missing event IDs
- Missing customer IDs
- Invalid timestamps
- Invalid or negative amounts
- Unknown customer IDs
- Duplicate event IDs

Invalid records are written to the quarantine output.

## 11. Output Artifacts

Each successful run produces:
- accepted_events
- quarantine
- baseline_result
- salted_result
- benchmark.json
- run_metrics.json

## 12. Correctness and Reconciliation

The pipeline compares baseline and salted aggregation results.

It also checks that accepted and rejected record counts reconcile with the input record count.

The benchmark results include correctness and reconciliation status.

## 13. Limitations

- Benchmarks were executed locally using Spark in local mode.
- Runtime can vary with JVM warm-up and machine resources.
- Results may differ on a multi-node Spark cluster.
- The benchmark does not establish that salting is faster for every workload.
