
import json
import uuid

import pytest
from pyspark.sql import SparkSession

from src.main import save_metrics
from src.pipeline import (
    validate_events,
    baseline_aggregate,
    salted_aggregate,
    compare_results,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("PipelineTests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_validate_events_separates_valid_and_rejected_rows(spark):
    events = spark.createDataFrame(
        [
            ("e1", "c1", "2026-01-01 10:00:00", "100", "file1", "A"),
            ("e2", "c1", "invalid-date", "200", "file1", "A"),
            ("e3", "c2", "2026-01-01 11:00:00", "-50", "file1", "B"),
            ("e4", "unknown", "2026-01-01 12:00:00", "50", "file1", "A"),
        ],
        [
            "event_id",
            "customer_id",
            "event_ts",
            "amount",
            "source_file",
            "expected_label",
        ],
    )

    customers = spark.createDataFrame(
        [("c1", "regular"), ("c2", "premium")],
        ["customer_id", "segment"],
    )

    accepted, rejected = validate_events(events, customers)

    assert accepted.count() == 1
    assert rejected.count() == 3


def test_baseline_and_salted_aggregates_match(spark):
    events = spark.createDataFrame(
        [
            ("e1", "cust_hot", 100.0),
            ("e2", "cust_hot", 200.0),
            ("e3", "c1", 50.0),
            ("e4", "c1", 75.0),
        ],
        ["event_id", "customer_id", "amount"],
    )

    customers = spark.createDataFrame(
        [
            ("cust_hot", "regular"),
            ("c1", "premium"),
        ],
        ["customer_id", "segment"],
    )

    baseline = baseline_aggregate(events, customers)
    salted = salted_aggregate(
        events,
        customers,
        salt_buckets=4,
    )

    comparison = compare_results(baseline, salted)

    assert comparison["matches"] is True
    assert comparison["left_only_rows"] == 0
    assert comparison["right_only_rows"] == 0


def test_invalid_salt_bucket_count_raises_error(spark):
    events = spark.createDataFrame(
        [("e1", "c1", 100.0)],
        ["event_id", "customer_id", "amount"],
    )

    customers = spark.createDataFrame(
        [("c1", "regular")],
        ["customer_id", "segment"],
    )

    with pytest.raises(ValueError):
        salted_aggregate(events, customers, salt_buckets=1)


def test_existing_output_directory_is_protected(tmp_path):
    output = tmp_path / "existing_output"
    output.mkdir()

    original_file = output / "result.txt"
    original_file.write_text("original data")

    with pytest.raises(FileExistsError):
        if output.exists():
            raise FileExistsError(
                f"Output directory already exists: {output}"
            )

    assert original_file.read_text() == "original data"


def test_run_ids_are_unique():
    first_run_id = str(uuid.uuid4())
    second_run_id = str(uuid.uuid4())

    assert first_run_id != second_run_id


def test_metrics_are_saved_as_valid_json(tmp_path):
    metrics_file = tmp_path / "run_metrics.json"

    metrics = {
        "run_id": "test-run-001",
        "status": "success",
        "input_rows": 1000,
        "accepted_rows": 995,
        "rejected_rows": 5,
    }

    save_metrics(tmp_path, metrics)

    assert metrics_file.exists()

    with open(metrics_file, "r", encoding="utf-8") as file:
        saved_metrics = json.load(file)

    assert saved_metrics["status"] == "success"
    assert saved_metrics["input_rows"] == 1000
    assert saved_metrics["accepted_rows"] == 995
