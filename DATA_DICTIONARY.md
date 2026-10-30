
# Data Dictionary

## 1. Overview

This document describes the fields used in the synthetic Spark skew optimization dataset.

The project contains two CSV files:

- events.csv
- customers.csv

## 2. Events Dataset

| Column | Data Type | Description |
|---|---|---|
| event_id | String | Unique identifier for each event |
| customer_id | String | Customer associated with the event |
| event_ts | Timestamp | Date and time when the event occurred |
| amount | Decimal/Double | Transaction amount |
| source_file | String | Source file associated with the event |
| expected_label | String | Expected category or label for the event |

## 3. Customers Dataset

| Column | Data Type | Description |
|---|---|---|
| customer_id | String | Unique customer identifier |
| segment | String | Customer segment |

## 4. Skewed Key Distribution

The generator assigns approximately 80% of events to one customer key:

- Hot key: cust_hot
- Remaining keys: cust_00001 to cust_00500

This creates an uneven key distribution for testing Spark partition skew.

## 5. Data Quality Rules

The pipeline validates the following:

1. Event ID must not be null or blank.
2. Customer ID must not be null or blank.
3. Event timestamp must be valid.
4. Amount must be numeric, finite, and non-negative.
5. Customer ID must exist in the customer dimension.
6. Duplicate event IDs are rejected.

## 6. Output Datasets

| Output | Description |
|---|---|
| accepted_events | Records that pass validation |
| quarantine | Records rejected during validation |
| baseline_result | Aggregation without salting |
| salted_result | Aggregation using salting |

## 7. Reconciliation

The pipeline checks:

Input records = Accepted records + Rejected records

It also compares baseline and salted aggregation results to verify correctness.
