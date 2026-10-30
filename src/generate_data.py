
import argparse
import csv
import random
from pathlib import Path
from datetime import datetime, timedelta, timezone


def generate(rows, output, seed=42, hot_key_fraction=0.8):
    if rows < 0:
        raise ValueError("rows must be >= 0")
    if not 0 <= hot_key_fraction <= 1:
        raise ValueError("hot_key_fraction must be between 0 and 1")

    rng = random.Random(seed)
    out = Path(output)
    out.mkdir(parents=True, exist_ok=True)

    customers = [
        ("cust_hot", "enterprise")
    ] + [
        (f"cust_{i:05d}",
         ["consumer", "small_business", "enterprise"][i % 3])
        for i in range(1, 501)
    ]

    with (out / "customers.csv").open(
        "w", newline="", encoding="utf-8"
    ) as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "segment"])
        writer.writerows(customers)

    events = []
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    hot_n = int(rows * hot_key_fraction)

    for i in range(rows):
        customer_id = (
            "cust_hot" if i < hot_n
            else f"cust_{rng.randint(1, 500):05d}"
        )

        event_time = start + timedelta(
            seconds=rng.randint(0, max(1, rows * 3))
        )

        amount = round(rng.uniform(0, 1000), 2)

        events.append([
            f"evt_{i:09d}",
            customer_id,
            event_time.isoformat().replace("+00:00", "Z"),
            amount,
            "events.csv",
            "valid"
        ])

    # Inject deliberate data-quality failures.
    if rows >= 10:
        events[1][2] = "not-a-timestamp"
        events[2][3] = -5
        events[3][1] = "cust_missing"
        events[4][1] = ""
        events[5][3] = ""
        events.append(events[6].copy())  # duplicate event ID

    with (out / "events.csv").open(
        "w", newline="", encoding="utf-8"
    ) as f:
        writer = csv.writer(f)
        writer.writerow([
            "event_id", "customer_id", "event_ts",
            "amount", "source_file", "expected_label"
        ])
        writer.writerows(events)

    return len(events), len(customers)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--output", default="data/sample")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--hot-key-fraction", type=float, default=0.8)
    args = parser.parse_args()

    event_count, customer_count = generate(
        args.rows, args.output, args.seed,
        args.hot_key_fraction
    )

    print(f"Generated {event_count} event rows")
    print(f"Generated {customer_count} customer rows")
