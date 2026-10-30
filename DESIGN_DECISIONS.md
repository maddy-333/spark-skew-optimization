# Design Decisions – Spark Skew Optimization Pipeline

## 1. Project Objective

The objective is to create a reproducible Spark workload with an uneven key distribution, diagnose data skew, implement salting as a mitigation, and compare runtime and correctness.

## 2. Technology Selection

### Apache Spark / PySpark
PySpark was selected because it supports distributed data processing, DataFrame transformations, joins, aggregations, and partition-level analysis.

### Python
Python was selected for data generation, pipeline orchestration, benchmarking, and automated testing.

### CSV
CSV was selected as a simple, portable input format for the synthetic event and customer datasets.

### Pytest
Pytest was used to validate data generation, pipeline behavior, aggregation correctness, and supporting functionality.

## 3. Synthetic Data Distribution

A configurable hot-key fraction was used to create an uneven distribution.

The default hot-key fraction is 80%.

The key `cust_hot` receives approximately 80% of the generated events, while the remaining events are distributed across other customer IDs.

This creates a reproducible workload for studying key concentration.

## 4. Baseline Aggregation

The baseline implementation joins events with the customer dimension using customer ID and groups the data by customer ID and segment.

This serves as the reference implementation for comparing salted aggregation.

## 5. Salting Strategy

The salted implementation assigns a salt value to events associated with the hot key.

The salt is calculated using a hash of the event ID and a configurable number of salt buckets.

Non-hot customer keys receive salt value zero.

This spreads records associated with the hot key across multiple key combinations.

## 6. Dimension Replication

The customer dimension row for the hot key is replicated across all configured salt buckets.

This allows salted event records to join with the corresponding customer dimension entry using both customer ID and salt.

After the join, partial aggregation results are combined by customer ID and segment.

## 7. Salt Bucket Experiments

Three configurations were tested:

- 8 salt buckets
- 16 salt buckets
- 32 salt buckets

The purpose was to compare partition distribution and execution time under different salting configurations.

## 8. Correctness Strategy

Performance optimization must preserve the aggregation result.

The implementation compares baseline and salted outputs using bidirectional set comparison.

It also checks record reconciliation:

Input records = Accepted records + Rejected records

A successful run requires matching aggregation results and successful reconciliation.

## 9. Data Quality Strategy

Invalid records are separated into a quarantine output.

Validation covers:

- Missing event IDs
- Missing customer IDs
- Invalid timestamps
- Invalid amounts
- Negative amounts
- Non-finite amounts
- Unknown customer IDs
- Duplicate event IDs

## 10. Benchmarking Strategy

Execution time is measured separately for baseline and salted aggregation.

Repeated benchmark trials are used to calculate median execution times.

Metrics are stored in JSON and CSV outputs for later analysis.

## 11. Performance Trade-offs

The experiments showed that salting reduced the largest partition in the repartition diagnostic by approximately 72.2%.

However, salted aggregation was slower than baseline aggregation in the tested local environment.

Potential overhead includes:

- Hash calculation
- Dimension replication
- Additional grouping by salt
- Final aggregation to combine partial results

Therefore, salting should be applied based on observed workload characteristics rather than assumed to improve every workload.

## 12. Known Limitations

- The benchmark was executed in local Spark mode rather than a distributed cluster.
- Partition distribution in the diagnostic is not identical to the aggregation join stage.
- Runtime can vary due to JVM warm-up and system resources.
- More experiments are required to evaluate performance on larger distributed workloads.

## 13. Possible Future Improvements

- Evaluate the pipeline on a multi-node Spark cluster.
- Compare salting with Adaptive Query Execution.
- Test different skew percentages and key distributions.
- Explore dynamic salt bucket selection.
- Evaluate Parquet input and output.
- Add more detailed stage-level Spark metrics.
