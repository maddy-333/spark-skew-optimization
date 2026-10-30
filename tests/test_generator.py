
import csv
from pathlib import Path

import pytest
from src.generate_data import generate


def test_generate_creates_csv_files(tmp_path):
    event_count, customer_count = generate(
        100, tmp_path, seed=42
    )

    assert event_count == 101  # 100 events + 1 duplicate
    assert customer_count == 501
    assert (tmp_path / "events.csv").exists()
    assert (tmp_path / "customers.csv").exists()


def test_generate_creates_expected_columns(tmp_path):
    generate(20, tmp_path)

    with open(tmp_path / "events.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == [
            "event_id", "customer_id", "event_ts",
            "amount", "source_file", "expected_label"
        ]

    with open(tmp_path / "customers.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == ["customer_id", "segment"]


def test_generate_injects_data_quality_errors(tmp_path):
    generate(20, tmp_path)

    with open(tmp_path / "events.csv", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert rows[1]["event_ts"] == "not-a-timestamp"
    assert rows[2]["amount"] == "-5"
    assert rows[3]["customer_id"] == "cust_missing"
    assert rows[4]["customer_id"] == ""
    assert rows[5]["amount"] == ""

    # The last row is a duplicate of an earlier event.
    assert rows[-1]["event_id"] == rows[6]["event_id"]


def test_generate_rejects_negative_rows(tmp_path):
    with pytest.raises(ValueError):
        generate(-1, tmp_path)


def test_generate_rejects_invalid_hot_key_fraction(tmp_path):
    with pytest.raises(ValueError):
        generate(100, tmp_path, hot_key_fraction=1.5)
