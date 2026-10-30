
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


EVENT_COLUMNS = [
    "event_id", "customer_id", "event_ts",
    "amount", "source_file", "expected_label"
]

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType(), True),
    T.StructField("customer_id", T.StringType(), True),
    T.StructField("event_ts", T.StringType(), True),
    T.StructField("amount", T.StringType(), True),
    T.StructField("source_file", T.StringType(), True),
    T.StructField("expected_label", T.StringType(), True),
])

CUSTOMER_SCHEMA = T.StructType([
    T.StructField("customer_id", T.StringType(), True),
    T.StructField("segment", T.StringType(), True),
])


def load_csv(spark, path, schema):
    return (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(path)
    )


def validate_events(events, customers):
    parsed = (
        events
        .withColumn("event_ts_parsed", F.to_timestamp("event_ts"))
        .withColumn("amount_parsed", F.expr("try_cast(amount as double)"))
        .withColumn(
            "reject_reason",
            F.when(
                F.col("event_id").isNull()
                | (F.trim("event_id") == ""),
                "missing_event_id"
            )
            .when(
                F.col("customer_id").isNull()
                | (F.trim("customer_id") == ""),
                "missing_customer_id"
            )
            .when(
                F.col("event_ts_parsed").isNull(),
                "invalid_event_ts"
            )
            .when(
                F.col("amount_parsed").isNull(),
                "invalid_amount"
            )
            .when(
                F.isnan("amount_parsed")
                | (F.abs("amount_parsed") == float("inf")),
                "non_finite_amount"
            )
            .when(
                F.col("amount_parsed") < 0,
                "negative_amount"
            )
        )
    )

    known_customers = (
        customers.select("customer_id")
        .dropDuplicates()
        .withColumn("_known_customer", F.lit(True))
    )

    parsed = parsed.join(
        known_customers, on="customer_id", how="left"
    )

    parsed = parsed.withColumn(
        "reject_reason",
        F.when(
            F.col("reject_reason").isNull()
            & F.col("_known_customer").isNull(),
            "unknown_customer"
        ).otherwise(F.col("reject_reason"))
    )

    valid = parsed.filter(F.col("reject_reason").isNull())

    # Keep one deterministic occurrence of each event ID.
    window = (
        Window.partitionBy("event_id")
        .orderBy(
            F.col("event_ts_parsed").asc(),
            F.col("source_file").asc_nulls_last()
        )
    )

    ranked = valid.withColumn(
        "_duplicate_rank", F.row_number().over(window)
    )

    duplicate_rejects = (
        ranked.filter(F.col("_duplicate_rank") > 1)
        .withColumn("reject_reason", F.lit("duplicate_event_id"))
    )

    accepted = ranked.filter(
        F.col("_duplicate_rank") == 1
    ).select(
        "event_id",
        "customer_id",
        F.col("event_ts_parsed").alias("event_ts"),
        F.col("amount_parsed").alias("amount"),
        "source_file",
        "expected_label"
    )

    rejected = (
        parsed.filter(F.col("reject_reason").isNotNull())
        .select(*EVENT_COLUMNS, "reject_reason")
        .unionByName(
            duplicate_rejects.select(
                *EVENT_COLUMNS, "reject_reason"
            )
        )
    )

    return accepted, rejected


def baseline_aggregate(events, customers):
    return (
        events.join(customers, "customer_id", "inner")
        .groupBy("customer_id", "segment")
        .agg(
            F.count("*").alias("event_count"),
            F.round(F.sum("amount"), 2).alias("total_amount")
        )
    )


def salted_aggregate(
    events, customers, salt_buckets=16, hot_key="cust_hot"
):
    if salt_buckets < 2:
        raise ValueError("salt_buckets must be >= 2")

    # Distribute events for the hot customer across salt buckets.
    salted_events = events.withColumn(
        "salt",
        F.when(
            F.col("customer_id") == hot_key,
            F.pmod(
                F.xxhash64("event_id"), F.lit(salt_buckets)
            )
        ).otherwise(F.lit(0))
    )

    # Replicate the hot customer's dimension row to every bucket.
    salted_customers = customers.withColumn(
        "salt",
        F.explode(
            F.when(
                F.col("customer_id") == hot_key,
                F.sequence(
                    F.lit(0), F.lit(salt_buckets - 1)
                )
            ).otherwise(F.array(F.lit(0)))
        )
    )

    # Join on customer ID and salt, then aggregate back.
    return (
        salted_events.join(
            salted_customers, ["customer_id", "salt"], "inner"
        )
        .groupBy("customer_id", "segment")
        .agg(
            F.count("*").alias("event_count"),
            F.round(F.sum("amount"), 2).alias("total_amount")
        )
    )


def compare_results(left, right):
    columns = [
        "customer_id", "segment", "event_count", "total_amount"
    ]

    left_only = left.select(*columns).exceptAll(
        right.select(*columns)
    ).count()

    right_only = right.select(*columns).exceptAll(
        left.select(*columns)
    ).count()

    return {
        "left_only_rows": left_only,
        "right_only_rows": right_only,
        "matches": left_only == 0 and right_only == 0
    }
