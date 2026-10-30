
# Spark Skew Optimization - Benchmark Report

## 1. Objective

Evaluate Spark data skew using a synthetic dataset and compare baseline aggregation with salted aggregation.

## 2. Experimental Setup

- Spark: PySpark 3.5.9
- Java: 17
- Python: 3.12
- Execution: Spark local mode
- Shuffle partitions: 8
- Hot-key fraction: 80%
- Random seed: 42

## 3. Dataset Sizes

| Dataset | Input Event Rows |
|---|---:|
| 100K | 100,001 |
| 500K | 500,001 |
| 1M | 1,000,001 |

The input counts include one deliberately injected duplicate record.

## 4. Baseline and Salted Runtime

Repeated trials on the 1M dataset:

| Salt Buckets | Baseline Median (sec) | Salted Median (sec) |
|---|---:|---:|
| 8 | 1.9592 | 2.4547 |
| 16 | 1.9669 | 2.5116 |
| 32 | 1.7863 | 2.4533 |

## 5. Runtime Analysis

The salted aggregation was slower than the baseline in all three configurations.

The additional processing includes:
- Hash-based salt calculation
- Replication of the hot-key dimension row
- Partial aggregation by salt
- Final aggregation to combine salted results

For this local workload, the extra processing cost exceeded the benefit of reducing key concentration.

## 6. Skew Diagnosis

For the 1M dataset, the baseline repartition diagnostic showed:

- Largest partition: 825,644 rows

After salting:

- Largest partition: 229,356 rows

This represents approximately 72.2% reduction in the largest partition.

The salted distribution was improved but remained uneven.

## 7. Data Quality Results

For the 1M pipeline run:

| Metric | Count |
|---|---:|
| Input rows | 1,000,001 |
| Accepted rows | 999,995 |
| Rejected rows | 6 |
| Customer records | 501 |

## 8. Correctness Results

- Baseline and salted aggregation results matched.
- No left-only result rows.
- No right-only result rows.
- Reconciliation passed.
- All repeated benchmark trials passed correctness and reconciliation checks.

## 9. Conclusion

Salting successfully reduced the largest partition in the repartition diagnostic by approximately 72.2%.

However, salted aggregation did not improve runtime in this local Spark environment.

The baseline aggregation remained faster for the tested workload.

This experiment demonstrates that salting can improve data distribution, but its performance benefit depends on workload size, cluster resources, shuffle behavior, and implementation overhead.

## 10. Limitations

- Experiments were conducted in local Spark mode.
- Runtime measurements may vary due to JVM warm-up and system load.
- Results may differ in a distributed cluster.
- The partition diagnostic measures repartition distribution, not the exact join stage of the aggregation pipeline.

