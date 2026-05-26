import json
import os
import logging
from typing import Iterator
import redis
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, LongType, TimestampType
)

# ── Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SPARK] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────
KAFKA_BROKER: str  = os.getenv("KAFKA_BROKER",  "aqi-kafka:29092")
KAFKA_TOPIC: str   = os.getenv("KAFKA_TOPIC",   "aqi-stream")
REDIS_HOST: str    = os.getenv("REDIS_HOST",    "aqi-redis")
REDIS_PORT: int    = int(os.getenv("REDIS_PORT", "6379"))
WINDOW_DURATION: str = os.getenv("WINDOW_DURATION", "30 seconds")
WATERMARK_DELAY: str = os.getenv("WATERMARK_DELAY", "10 seconds")
CHECKPOINT_DIR: str  = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoint")

# ── Schema for incoming JSON messages ───────────────────────────
MESSAGE_SCHEMA: StructType = StructType([
    StructField("state",      StringType(),  True),
    StructField("city",       StringType(),  True),
    StructField("date",       StringType(),  True),
    StructField("aqi",        DoubleType(),  True),
    StructField("category",   StringType(),  True),
    StructField("band",       StringType(),  True),
    StructField("parameter",  StringType(),  True),
    StructField("lat",        DoubleType(),  True),
    StructField("lng",        DoubleType(),  True),
    StructField("population", DoubleType(),  True),
    StructField("timestamp",  DoubleType(),  True),
])

# ── AQI band classifier ──────────────────────────────────────────
def classify_aqi(aqi_value: float) -> str:
    if aqi_value <= 50:
        return "Good"
    elif aqi_value <= 100:
        return "Moderate"
    elif aqi_value <= 150:
        return "Unhealthy for Sensitive Groups"
    elif aqi_value <= 200:
        return "Unhealthy"
    elif aqi_value <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"

# ── Write micro-batch to Redis ───────────────────────────────────
def write_to_redis(batch_df: DataFrame, batch_id: int) -> None:
    log.info(f"Processing batch_id={batch_id}")
    rows = batch_df.collect()
    if not rows:
        log.info(f"Batch {batch_id} is empty — skipping")
        return

    r: redis.Redis = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True
    )

    pipe = r.pipeline()

    for row in rows:
        avg_aqi: float = round(float(row["avg_aqi"]), 2)
        band: str = classify_aqi(avg_aqi)
        window_start: str = str(row["window_start"])
        state: str = str(row["state"])

        # Store windowed result as Redis hash
        key: str = f"window:{state}:{window_start}"
        pipe.hset(key, mapping={
            "state":       state,
            "window_start": window_start,
            "avg_aqi":     avg_aqi,
            "max_aqi":     round(float(row["max_aqi"]), 2),
            "min_aqi":     round(float(row["min_aqi"]), 2),
            "record_count": int(row["record_count"]),
            "band":        band,
            "batch_id":    batch_id
        })
        pipe.expire(key, 3600)

        # Keep a sorted set of latest windows for dashboard
        pipe.zadd("latest_windows", {key: float(row["window_ts"])})
        pipe.zremrangebyrank("latest_windows", 0, -501)

        # Band counter for classification breakdown
        pipe.hincrby("band_counts", band, int(row["record_count"]))

        # Recent records feed
        record_json: str = json.dumps({
            "state":   state,
            "avg_aqi": avg_aqi,
            "band":    band,
            "window":  window_start,
            "count":   int(row["record_count"])
        })
        pipe.lpush("recent_records", record_json)
        pipe.ltrim("recent_records", 0, 99)

    pipe.execute()
    log.info(f"Batch {batch_id} — wrote {len(rows)} windows to Redis")

# ── Write sliding window micro-batch to Redis ────────────────────
def write_sliding_to_redis(batch_df: DataFrame, batch_id: int) -> None:
    rows = batch_df.collect()
    if not rows:
        return

    r: redis.Redis = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True
    )
    pipe = r.pipeline()

    for row in rows:
        avg_aqi: float = round(float(row["avg_aqi"]), 2)
        band: str = classify_aqi(avg_aqi)
        window_start: str = str(row["window_start"])
        state: str = str(row["state"])

        key: str = f"sliding:{state}:{window_start}"
        pipe.hset(key, mapping={
            "state":        state,
            "window_start": window_start,
            "avg_aqi":      avg_aqi,
            "record_count": int(row["record_count"]),
            "band":         band,
            "batch_id":     batch_id
        })
        pipe.expire(key, 3600)
        pipe.zadd("latest_sliding", {key: float(row["window_ts"])})
        pipe.zremrangebyrank("latest_sliding", 0, -501)

    pipe.execute()

# ── Spark Session ────────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("AQI_Structured_Streaming")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

# ── Main ─────────────────────────────────────────────────────────
def main() -> None:
    spark: SparkSession = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session created")

    # ── Read from Kafka ──────────────────────────────────────────
    raw_stream: DataFrame = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON ───────────────────────────────────────────────
    parsed: DataFrame = (
        raw_stream
        .select(
            F.from_json(
                F.col("value").cast("string"),
                MESSAGE_SCHEMA
            ).alias("data")
        )
        .select("data.*")
        .withColumn(
            "event_time",
            F.to_timestamp(F.from_unixtime(F.col("timestamp")))
        )
        .filter(F.col("aqi").isNotNull())
    )

    # ── Apply watermark ──────────────────────────────────────────
    watermarked: DataFrame = parsed.withWatermark(
        "event_time", WATERMARK_DELAY
    )

    # ── Tumbling window aggregation ──────────────────────────────
    tumbling: DataFrame = (
        watermarked
        .groupBy(
            F.window(F.col("event_time"), WINDOW_DURATION).alias("win"),
            F.col("state")
        )
        .agg(
            F.avg("aqi").alias("avg_aqi"),
            F.max("aqi").alias("max_aqi"),
            F.min("aqi").alias("min_aqi"),
            F.count("*").alias("record_count")
        )
        .select(
            F.col("state"),
            F.col("win.start").alias("window_start"),
            F.col("win.end").alias("window_end"),
            F.col("avg_aqi"),
            F.col("max_aqi"),
            F.col("min_aqi"),
            F.col("record_count"),
            F.unix_timestamp(F.col("win.start")).alias("window_ts")
        )
    )

    # ── Sliding window aggregation (bonus) ───────────────────────
    sliding: DataFrame = (
        watermarked
        .groupBy(
            F.window(F.col("event_time"), "60 seconds", "15 seconds").alias("win"),
            F.col("state")
        )
        .agg(
            F.avg("aqi").alias("avg_aqi"),
            F.count("*").alias("record_count")
        )
        .select(
            F.col("state"),
            F.col("win.start").alias("window_start"),
            F.col("win.end").alias("window_end"),
            F.col("avg_aqi"),
            F.col("record_count"),
            F.unix_timestamp(F.col("win.start")).alias("window_ts")
        )
    )

    # ── Write tumbling stream to Redis ───────────────────────────
    tumbling_query = (
        tumbling.writeStream
        .outputMode("update")
        .foreachBatch(write_to_redis)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/tumbling")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # ── Write sliding stream to Redis ────────────────────────────
    sliding_query = (
        sliding.writeStream
        .outputMode("update")
        .foreachBatch(write_sliding_to_redis)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/sliding")
        .trigger(processingTime="10 seconds")
        .start()
    )

    log.info("Both streaming queries started — waiting for data")
    log.info(f"Tumbling window: {WINDOW_DURATION} | Watermark: {WATERMARK_DELAY}")
    log.info("Sliding window: 60 seconds / 15 second slide")

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
