
import argparse
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

from src.pipeline import (
    load_csv,
    EVENT_SCHEMA,
    CUSTOMER_SCHEMA,
    validate_events,
    baseline_aggregate,
    salted_aggregate,
    compare_results,
)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def save_metrics(output, metrics):
    """Save run metrics safely as JSON."""
    metrics_path = output / "run_metrics.json"
    temp_path = output / "run_metrics.tmp"

    with temp_path.open("w", encoding="utf-8") as file:
        json.dump(metrics, file, indent=2)

    temp_path.replace(metrics_path)


def main():
    parser = argparse.ArgumentParser(
        description="Spark skew diagnosis and mitigation pipeline"
    )

    parser.add_argument("--input", required=True)
    parser.add_argument("--dimension", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--salt-buckets", type=int, default=16)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--shuffle-partitions", type=int, default=8)

    args = parser.parse_args()

    # Validate configuration
    if args.salt_buckets < 2:
        parser.error("--salt-buckets must be at least 2")

    if args.shuffle_partitions < 1:
        parser.error("--shuffle-partitions must be at least 1")

    run_id = args.run_id or str(uuid.uuid4())
    output = Path(args.output)

    # Protect existing benchmark results
    if output.exists():
        raise FileExistsError(
            f"Output directory already exists: {output}. "
            "Choose a new output directory."
        )

    output.mkdir(parents=True, exist_ok=False)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    started_at = utc_now()
    start = time.perf_counter()

    metrics = {
        "run_id": run_id,
        "status": "running",
        "started_at": started_at,
        "input_path": args.input,
        "dimension_path": args.dimension,
        "salt_buckets": args.salt_buckets,
        "shuffle_partitions": args.shuffle_partitions,
    }

    spark = None
    baseline = None
    salted = None

    # Record the run before starting Spark
    save_metrics(output, metrics)

    try:
        logging.info("Starting Spark pipeline: run_id=%s", run_id)

        spark = (
            SparkSession.builder
            .appName("spark-skew-pipeline")
            .config(
                "spark.sql.shuffle.partitions",
                str(args.shuffle_partitions)
            )
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        # Load input data
        logging.info("Loading input datasets")

        raw = load_csv(spark, args.input, EVENT_SCHEMA)

        customers = load_csv(
            spark,
            args.dimension,
            CUSTOMER_SCHEMA
        )

        # Validate event schema
        expected_columns = set(EVENT_SCHEMA.fieldNames())

        if set(raw.columns) != expected_columns:
            raise ValueError("Event schema mismatch")

        input_count = raw.count()

        # Validate customer dimension
        if customers.filter(
            "customer_id IS NULL OR segment IS NULL"
        ).limit(1).count():
            raise ValueError("Null customer key or segment")

        if customers.groupBy("customer_id").count().filter(
            "count > 1"
        ).limit(1).count():
            raise ValueError("Duplicate customer dimension key")

        # Data validation and quarantine
        logging.info("Validating event records")

        accepted, rejected = validate_events(raw, customers)

        accepted_count = accepted.count()
        rejected_count = rejected.count()

        accepted.write.mode("errorifexists").parquet(
            str(output / "accepted_events")
        )

        rejected.write.mode("errorifexists").parquet(
            str(output / "quarantine")
        )

        # Baseline aggregation
        logging.info("Running baseline aggregation")

        t0 = time.perf_counter()

        baseline = baseline_aggregate(
            accepted, customers
        ).cache()

        baseline_count = baseline.count()
        baseline_seconds = time.perf_counter() - t0

        # Salted aggregation
        logging.info("Running salted aggregation")

        t1 = time.perf_counter()

        salted = salted_aggregate(
            accepted,
            customers,
            args.salt_buckets
        ).cache()

        salted_count = salted.count()
        salted_seconds = time.perf_counter() - t1

        # Correctness comparison
        logging.info("Comparing aggregation results")

        comparison = compare_results(baseline, salted)

        if not comparison["matches"]:
            raise AssertionError(
                f"Baseline and salted outputs differ: {comparison}"
            )

        # Save final results
        baseline.write.mode("errorifexists").parquet(
            str(output / "baseline_result")
        )

        salted.write.mode("errorifexists").parquet(
            str(output / "salted_result")
        )

        reconciliation = (
            accepted_count + rejected_count == input_count
        )

        if not reconciliation:
            raise AssertionError(
                "Accepted and rejected counts do not equal input count"
            )

        # Successful run metrics
        metrics.update({
            "status": "success",
            "input_rows": input_count,
            "accepted_rows": accepted_count,
            "rejected_rows": rejected_count,
            "target_rows": baseline_count,
            "salted_target_rows": salted_count,
            "baseline_seconds": round(baseline_seconds, 4),
            "salted_seconds": round(salted_seconds, 4),
            "speedup_baseline_over_salted": (
                round(baseline_seconds / salted_seconds, 4)
                if salted_seconds else None
            ),
            "comparison": comparison,
            "accepted_plus_rejected_equals_input": reconciliation,
            "elapsed_seconds": round(
                time.perf_counter() - start, 4
            ),
            "ended_at": utc_now(),
        })

        # Save benchmark summary
        with (output / "benchmark.json").open(
            "w", encoding="utf-8"
        ) as file:
            json.dump({
                "baseline_seconds": baseline_seconds,
                "salted_seconds": salted_seconds,
                "comparison": comparison,
            }, file, indent=2)

        save_metrics(output, metrics)

        logging.info(
            "Pipeline completed successfully: %s",
            json.dumps(metrics)
        )

    except Exception as exc:
        metrics.update({
            "status": "failed",
            "failure_reason": repr(exc),
            "elapsed_seconds": round(
                time.perf_counter() - start, 4
            ),
            "ended_at": utc_now(),
        })

        save_metrics(output, metrics)

        logging.exception(
            "Pipeline failed: run_id=%s",
            run_id
        )

        raise

    finally:
        if baseline is not None:
            baseline.unpersist()

        if salted is not None:
            salted.unpersist()

        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()

