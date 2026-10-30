
# Spark Skew Optimization Architecture

## 1. Architecture Overview

The pipeline generates synthetic event data with a skewed customer key distribution, validates the records, performs baseline and salted aggregations, and compares the results.

## 2. Data Flow

```text
                 Synthetic Data Generator
                           |
                  +--------+--------+
                  |                 |
              events.csv       customers.csv
                  |                 |
                  +--------+--------+
                           |
                     Load into Spark
                           |
                    Data Validation
                           |
                 +---------+----------+
                 |                    |
           Accepted Records     Rejected Records
                 |                    |
                 |                Quarantine
                 |
          Aggregation Comparison
                 |
          +------+-------+
          |              |
     Baseline         Salted
    Aggregation     Aggregation
          |              |
          +------+-------+
                 |
          Result Comparison
                 |
       Correctness Validation
                 |
          Benchmark Metrics
                 |
       JSON and CSV Outputs
```

## 3. Pipeline Components

### Data Generator
Creates synthetic event and customer datasets with a configurable hot-key fraction and random seed.

### Data Loader
Loads CSV data into Spark DataFrames using explicit schemas.

### Data Validation
Checks required fields, timestamps, amounts, customer references, and duplicate event IDs.

### Baseline Aggregation
Joins events with customer information and groups records by customer ID and segment.

### Salted Aggregation
Splits the hot customer key into multiple salt buckets, replicates the corresponding customer dimension row, aggregates by customer and salt, and combines the partial results.

### Result Comparison
Uses bidirectional set comparison to check whether baseline and salted outputs match.

### Benchmarking
Measures execution time, records row counts, calculates speedup, and saves metrics.

## 4. Fault Handling

- Invalid records are written to quarantine.
- Existing output directories are protected from accidental overwrites.
- Pipeline failures are logged.
- Metrics record success or failure status.
- Cached Spark DataFrames are released and the Spark session is stopped in cleanup.

## 5. Execution Environment

- Python 3.12
- Java 17
- PySpark 3.5.9
- Ubuntu 24.04 on WSL2
- Spark local execution mode
