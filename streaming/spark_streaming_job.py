"""Structured Streaming job for transaction events.

Pipeline:
  1. Read raw events from Kafka (topic: transactions) and parse the JSON payload.
  2. Apply the five validations and tag each event with is_valid / error_reason.
  3. Route valid events to transactions_valid and invalid events (including
     duplicates and late events) to transactions_dlq.
  4. Monitor transactions_valid with a 1-minute tumbling window and a 3-minute
     watermark, printing the cumulative total to the console.
"""

import os
import time
from datetime import datetime
from pathlib import Path
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, TimestampType,
)
import sys

# Local Windows environment
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["PATH"] = "C:/hadoop/bin;" + os.environ["PATH"]
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-17.0.21.0.3"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

KAFKA_SERVERS = "localhost:9092"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
IVY_DIR = Path.home() / ".ivy2_clean"
CHECKPOINT_ROOT = "/tmp"

TOPIC_INPUT = "transactions"
TOPIC_VALID = "transactions_valid"
TOPIC_DLQ = "transactions_dlq"

VALID_SOURCES = ["mobile", "web", "pos"]
MIN_AMOUNT = 1.0
MAX_AMOUNT = 10_000_000.0

WATERMARK_DELAY = "3 minutes"
WATERMARK_SECONDS = 180
WINDOW_DURATION = "1 minute"

INPUT_SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True),
])

# Subset of the enriched payload that the monitoring query needs
VALID_SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("source", StringType(), True),
    StructField("event_time", TimestampType(), True),
])


class DuplicateTracker:
    """Detects duplicates by (user_id, timestamp) across micro-batches.

    Events older than the watermark are rejected as late before they reach this
    check, so a key never has to be remembered longer than the watermark delay.
    """

    def __init__(self, retention_seconds: int):
        self.retention_seconds = retention_seconds
        self.first_seen = {}

    def find_duplicates(self, rows) -> list:
        now = time.time()
        self.first_seen = {
            key: seen_at for key, seen_at in self.first_seen.items()
            if now - seen_at <= self.retention_seconds
        }
        duplicate_ids = []
        for row in rows:
            key = (row["user_id"], row["timestamp"])
            if key in self.first_seen:
                duplicate_ids.append(row["row_id"])
            else:
                self.first_seen[key] = now
        return duplicate_ids


tracker = DuplicateTracker(WATERMARK_SECONDS)


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("StreamingAssignmentPipeline")
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.jars.ivy", IVY_DIR.as_posix())
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def ensure_topics(topics: list) -> None:
    """Create the Kafka topics if they do not exist yet."""
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_SERVERS)
    for topic in topics:
        try:
            admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
        except TopicAlreadyExistsError:
            pass
    admin.close()


def read_kafka(spark: SparkSession, topic: str) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )


def read_transactions(spark: SparkSession) -> DataFrame:
    """Parse raw Kafka events into the defined schema."""
    return (
        read_kafka(spark, TOPIC_INPUT)
        .select(f.from_json(f.col("value").cast("string"), INPUT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", f.to_timestamp("timestamp"))
    )


def validate(df: DataFrame) -> DataFrame:
    """Add error_reason and is_valid using the stateless validations.

    Mandatory fields, type (timestamp parsing), amount range, source and
    lateness are checked here. Duplicates need state and are handled in
    route_batch.
    """
    late_cutoff = f.current_timestamp() - f.expr(f"INTERVAL {WATERMARK_DELAY}")
    error_reason = (
        f.when(f.col("user_id").isNull(), "Missing user_id")
        .when(f.col("amount").isNull(), "Missing amount")
        .when(f.col("timestamp").isNull(), "Missing timestamp")
        .when(f.col("source").isNull(), "Missing source")
        .when(f.col("event_time").isNull(), "Invalid timestamp format")
        .when(~f.col("amount").between(MIN_AMOUNT, MAX_AMOUNT), "Amount out of range")
        .when(~f.col("source").isin(VALID_SOURCES), "Unknown source")
        .when(f.col("event_time") < late_cutoff, "Late event (older than watermark)")
        .otherwise("Valid")
    )
    return (
        df.withColumn("error_reason", error_reason)
        .withColumn("is_valid", f.col("error_reason") == "Valid")
    )


def write_to_kafka(df: DataFrame, topic: str) -> None:
    (
        df.select(f.to_json(f.struct("*")).alias("value"))
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("topic", topic)
        .save()
    )


def route_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Flag duplicates, then send valid events and DLQ events to their topics."""
    batch_df = batch_df.withColumn("row_id", f.monotonically_increasing_id()).persist()

    valid_rows = (
        batch_df.filter("is_valid")
        .select("row_id", "user_id", "timestamp")
        .orderBy("row_id")
        .collect()
    )
    duplicate_ids = tracker.find_duplicates(valid_rows)
    is_duplicate = f.col("row_id").isin(duplicate_ids) if duplicate_ids else f.lit(False)

    checked = (
        batch_df
        .withColumn("error_reason", f.when(is_duplicate, "Duplicate event").otherwise(f.col("error_reason")))
        .withColumn("is_valid", f.col("is_valid") & ~is_duplicate)
        .drop("row_id")
    )

    write_to_kafka(checked.filter("is_valid"), TOPIC_VALID)
    write_to_kafka(checked.filter(~f.col("is_valid")), TOPIC_DLQ)

    counts = {row["error_reason"]: row["count"] for row in checked.groupBy("error_reason").count().collect()}
    valid = counts.pop("Valid", 0)
    print(f"[batch {batch_id}] valid={valid} dlq={sum(counts.values())} {counts}")

    batch_df.unpersist()


def build_window_monitor(spark: SparkSession) -> DataFrame:
    """Count valid transactions per 1-minute tumbling window."""
    return (
        read_kafka(spark, TOPIC_VALID)
        .select(f.from_json(f.col("value").cast("string"), VALID_SCHEMA).alias("data"))
        .select("data.*")
        .withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(f.window("event_time", WINDOW_DURATION))
        .count()
    )


def show_running_total(batch_df: DataFrame, batch_id: int) -> None:
    """Print per-window counts plus the cumulative total of valid transactions."""
    windows = batch_df.orderBy("window.start").collect()
    if not windows:
        return

    time_format = "%Y-%m-%d %H:%M:%S"
    output_time = datetime.now().strftime(time_format)
    running_total = 0
    table = []
    for row in windows:
        running_total += row["count"]
        table.append((
            output_time,
            row["window"]["start"].strftime(time_format),
            row["window"]["end"].strftime(time_format),
            row["count"],
            running_total,
        ))

    schema = "timestamp string, window_start string, window_end string, window_total long, running_total long"
    batch_df.sparkSession.createDataFrame(table, schema).show(truncate=False)


def main():
    ensure_topics([TOPIC_INPUT, TOPIC_VALID, TOPIC_DLQ])
    spark = create_spark_session()

    validated = validate(read_transactions(spark))
    (
        validated.writeStream
        .foreachBatch(route_batch)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/cp_routing")
        .start()
    )

    (
        build_window_monitor(spark).writeStream
        .outputMode("complete")
        .foreachBatch(show_running_total)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/cp_monitor")
        .start()
    )

    print("Spark streaming job is running...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()