# AI Usage Disclosure

## Project
Spark Skew Optimization Pipeline

## AI Tool Used
ChatGPT

## 1. Purpose of AI Assistance

I used ChatGPT as a learning, implementation-support, debugging, and documentation assistant during this assignment.

AI assistance helped me understand Spark concepts, resolve development environment issues, structure the implementation, and interpret experimental results.

## 2. Where AI Accelerated My Work

ChatGPT assisted with:

- Understanding data skew and the salting technique in Apache Spark.
- Planning the project structure and implementation stages.
- Setting up PySpark, Java, Python, and WSL2 Ubuntu.
- Understanding data validation and quarantine handling.
- Designing benchmark comparisons.
- Troubleshooting Python import and PySpark execution errors.
- Preparing project documentation and test cases.

## 3. Areas Where I Relied Heavily on AI

AI provided substantial implementation guidance and code assistance for:

- PySpark pipeline structure.
- Baseline and salted aggregation logic.
- Data validation and error handling.
- Benchmarking and metrics generation.
- Automated test implementation.
- Technical documentation.

I reviewed the generated code and used local execution to check its behavior. I acknowledge that AI contributed to implementation, and I do not claim that all code was independently written without assistance.

## 4. Work Personally Performed

I personally worked through the project by:

- Setting up the development environment in Ubuntu WSL2.
- Installing and configuring Python, Java, PySpark, and project dependencies.
- Running the synthetic data generator.
- Executing the Spark pipeline on different dataset sizes.
- Running skew diagnosis scripts.
- Comparing baseline and salted aggregation results.
- Running repeated benchmark trials.
- Executing the automated test suite.
- Inspecting execution outputs and benchmark metrics.
- Preparing the project documentation and submission files.

## 5. Validation and Testing

The implementation was tested locally using PySpark.

Validation included:

- 100K, 500K, and 1M input datasets.
- Baseline and salted aggregation comparison.
- Accepted and rejected record reconciliation.
- Partition distribution analysis.
- Repeated trials with 8, 16, and 32 salt buckets.
- Automated tests: 11 passed.

## 6. Key Finding

The skew diagnostic showed that the largest partition decreased from 825,644 rows to 229,356 rows after salting, approximately a 72.2% reduction.

However, the salted aggregation was slower than baseline aggregation in the local benchmark experiments.

This finding is documented rather than presenting salting as an automatic performance improvement.

## 7. Limitations

- The experiments were performed in local Spark mode using WSL2 Ubuntu.
- Runtime measurements may vary with system load and JVM warm-up.
- Distributed cluster performance was not evaluated.
- AI-generated implementation suggestions may require further optimization for production workloads.

## 8. Transparency Statement

ChatGPT was used throughout development for technical explanations, implementation assistance, debugging, testing guidance, and documentation.

The submitted prompt history is provided separately in `ai_usage/PROMPT_HISTORY.md`.

I understand that I am responsible for explaining the implementation, design choices, benchmark results, dependencies, and limitations of this project.
