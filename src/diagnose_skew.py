

from pyspark.sql import SparkSession, functions as F
from src.pipeline import load_csv, EVENT_SCHEMA

spark = (
    SparkSession.builder
    .appName("SkewDiagnosis")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

events = load_csv(
    spark,
    "data/benchmark_1m/events.csv",
    EVENT_SCHEMA
)

print("\n--- Customer key distribution ---")

events.groupBy("customer_id").count() \
    .orderBy(F.desc("count")) \
    .show(10, truncate=False)

print("\n--- Baseline partition distribution ---")

baseline = events.repartition(8, "customer_id") \
    .withColumn("partition_id", F.spark_partition_id())

baseline.groupBy("partition_id").count() \
    .orderBy("partition_id").show()

print("\n--- Salted partition distribution ---")

salted = events.withColumn(
    "salt",
    F.when(
        F.col("customer_id") == "cust_hot",
        F.pmod(F.xxhash64("event_id"), F.lit(16))
    ).otherwise(F.lit(0))
)

salted.repartition(8, "customer_id", "salt") \
    .withColumn("partition_id", F.spark_partition_id()) \
    .groupBy("partition_id").count() \
    .orderBy("partition_id").show()

spark.stop()



