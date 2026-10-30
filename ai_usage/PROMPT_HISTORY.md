# AI Prompt History
## Project: Spark Skew Optimization Pipeline
**AI Tool Used:** ChatGPT  
**Purpose:** Record of prompts used during project development.

**Disclosure:** This document is a reconstructed and organized record of the prompts used during the project. Some entries summarize the original requests rather than reproduce their exact wording. The complete conversation history should be consulted for any additional prompts.

---

## 1. Project Understanding and Planning

**Prompt 1**

Create a reproducible Spark workload with a skewed key distribution. Diagnose the bottleneck, implement a suitable mitigation, and report runtime and correctness comparisons.

**Prompt 2**

Help me understand the requirements of the Spark data engineering assignment and divide the implementation into manageable steps.

**Prompt 3**

Suggest a project structure for a PySpark data engineering solution that includes data generation, validation, skew diagnosis, aggregation, benchmarking, and testing.

## 2. Environment Setup and Troubleshooting

**Prompt 4**

Guide me through setting up PySpark, Python, Java, and the required dependencies on my local Windows environment.

**Prompt 5**

Help me troubleshoot the PySpark and winutils.exe errors on Windows.

**Prompt 6**

Guide me through moving my Spark project to Ubuntu using WSL2 and running it in a Python virtual environment.

**Prompt 7**

Explain how to activate the virtual environment and run Python modules from the project root.

## 3. Synthetic Data Generation

**Prompt 8**

Help me create a reproducible synthetic dataset with a configurable skewed customer key distribution.

**Prompt 9**

Generate a dataset with approximately 80% of events assigned to one hot customer key and the remaining events distributed across other customers.

**Prompt 10**

Include deliberate data-quality issues such as invalid timestamps, negative amounts, missing customer IDs, unknown customers, and duplicate event IDs.

**Prompt 11**

Explain how to generate 100K, 500K, and 1M event datasets using the same random seed.

## 4. Spark Pipeline Implementation

**Prompt 12**

Help me implement a PySpark pipeline that loads event and customer CSV files using explicit schemas.

**Prompt 13**

Implement data validation to separate accepted and rejected records.

**Prompt 14**

Help me implement baseline aggregation by joining event records with the customer dimension.

**Prompt 15**

Explain how to implement salted aggregation to distribute a hot key across multiple salt buckets.

**Prompt 16**

Help me compare baseline and salted aggregation results to verify that both approaches produce identical results.

## 5. Skew Diagnosis and Optimization

**Prompt 17**

Help me diagnose data skew by examining customer key distribution and Spark partition distribution.

**Prompt 18**

Implement a Spark diagnostic script to compare partition distribution before and after salting.

**Prompt 19**

Explain how to calculate the reduction in the largest partition after applying salting.

**Prompt 20**

Help me understand why salted aggregation can be slower than baseline aggregation in a local Spark environment.

**Prompt 21**

Explain how Spark execution plans, BroadcastHashJoin, Exchange, and HashAggregate help identify processing overhead.

## 6. Benchmarking and Performance Analysis

**Prompt 22**

Help me measure baseline and salted aggregation execution times and save benchmark metrics as JSON.

**Prompt 23**

Compare performance using 8, 16, and 32 salt buckets.

**Prompt 24**

Help me create a script to execute repeated benchmark trials and calculate median execution times.

**Prompt 25**

Help me interpret benchmark results and explain why salting improved partition distribution but did not improve runtime.

**Prompt 26**

Create a benchmark comparison report showing execution time, correctness, and reconciliation results.

## 7. Testing and Debugging

**Prompt 27**

Help me create pytest test cases for the synthetic data generator.

**Prompt 28**

Create tests for event validation, baseline and salted aggregation correctness, and invalid salt bucket configurations.

**Prompt 29**

Help me add tests for output directory protection, unique run IDs, and metrics JSON generation.

**Prompt 30**

I received a ModuleNotFoundError: No module named 'src' while running pytest. Help me troubleshoot the Python import path and virtual environment.

**Prompt 31**

Explain how to run all project tests using python -m pytest -v.

## 8. Documentation

**Prompt 32**

Help me prepare a README with project overview, technology stack, environment setup, data generation, execution instructions, and benchmark findings.

**Prompt 33**

Create a data dictionary explaining the event and customer dataset fields.

**Prompt 34**

Help me document the architecture and data flow of the Spark skew optimization pipeline.

**Prompt 35**

Create a benchmark report explaining runtime comparisons, skew reduction, correctness, and limitations.

## 9. Packaging and Submission

**Prompt 36**

Explain how to package the completed Spark project into a ZIP file without including the Python virtual environment.

**Prompt 37**

Help me copy the ZIP file from Ubuntu WSL2 into the Windows Downloads folder.

**Prompt 38**

Help me prepare the project for a public GitHub repository according to the recruitment submission guidelines.

**Prompt 39**

Help me prepare an AI usage disclosure and a complete prompt history for the project.

---

## 10. How AI Assisted This Project

ChatGPT was used for:

- Understanding Spark skew and salting concepts.
- Environment setup and troubleshooting.
- Implementation guidance and code generation.
- Debugging Python and PySpark errors.
- Developing test cases.
- Interpreting benchmark results.
- Preparing technical documentation and submission materials.

## 11. Candidate Review and Validation

The candidate should document the work personally performed, including:

- Executing the pipeline in the local Ubuntu WSL2 environment.
- Generating datasets of different sizes.
- Inspecting Spark partition
