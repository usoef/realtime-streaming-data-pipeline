import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["PATH"] = "C:/hadoop/bin;" + os.environ["PATH"]
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-17.0.21.0.3"
#os.environ["_JAVA_OPTIONS"] = "-Djava.net.preferIPv4Stack=true"

def main():
    spark = SparkSession.builder \
        .appName("StreamingAssignmentPipeline") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.jars.ivy", "C:/Users/yusuf.abdulloh/.ivy2_clean") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # 1. Baca dari Kafka
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transactions") \
        .option("startingOffsets", "latest") \
        .load()

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("source", StringType(), True)
    ])

    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(f.from_json(f.col("json_str"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("parsed_timestamp", f.to_timestamp("timestamp"))

    # 2. 5 Validasi Wajib
    cond_mandatory = f.col("user_id").isNotNull() & f.col("amount").isNotNull() & f.col("timestamp").isNotNull() & f.col("source").isNotNull()
    cond_type = f.col("parsed_timestamp").isNotNull()
    cond_amount = (f.col("amount") >= 1.0) & (f.col("amount") <= 10000000.0)
    cond_source = f.col("source").isin(["mobile", "web", "pos"])

    df_validated = df_parsed.withColumn(
        "is_valid", cond_mandatory & cond_type & cond_amount & cond_source
    ).withColumn(
        "error_reason",
        f.when(f.col("user_id").isNull(), "Missing user_id")
         .when(f.col("amount").isNull(), "Missing amount")
         .when(f.col("parsed_timestamp").isNull(), "Invalid timestamp format")
         .when((f.col("amount") < 1.0) | (f.col("amount") > 10000000.0), "Amount out of range")
         .when(~f.col("source").isin(["mobile", "web", "pos"]), "Unknown source")
         .otherwise("Valid")
    ).withColumn("event_time", f.coalesce(f.col("parsed_timestamp"), f.current_timestamp()))

    # 3. Routing Output (Valid vs DLQ)
    df_valid = df_validated.filter(f.col("is_valid") == True)
    df_invalid = df_validated.filter(f.col("is_valid") == False)

    # Tulis ke topik Kafka valid & DLQ
    q_valid = df_valid.select(f.to_json(f.struct("*")).alias("value")).writeStream \
        .format("kafka").option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "transactions_valid").option("checkpointLocation", "/tmp/cp_valid") \
        .outputMode("append").start()

    q_dlq = df_invalid.select(f.to_json(f.struct("*")).alias("value")).writeStream \
        .format("kafka").option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "transactions_dlq").option("checkpointLocation", "/tmp/cp_dlq") \
        .outputMode("append").start()

    # 4. Watermark (3 menit) & Tumbling Window (1 menit)
    df_windowed = df_valid \
        .withWatermark("event_time", "3 minutes") \
        .groupBy(f.window(f.col("event_time"), "1 minute"), f.col("source")) \
        .agg(f.count("user_id").alias("running_total"), f.sum("amount").alias("total_amount"))

    # 5. Output Console
    q_console = df_windowed.select(
        f.col("window.end").alias("timestamp"),
        f.col("source"),
        f.col("running_total"),
        # Mengubah total_amount menjadi tipe data Long/Bigint agar tampil angka utuh tanpa notasi E
        f.col("total_amount").cast("bigint").alias("total_amount")
    ).writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    print("🟢 Spark Streaming Job berjalan...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()