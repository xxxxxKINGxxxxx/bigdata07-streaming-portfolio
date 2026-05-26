"""
Assignment 1 — Task 1: Kafka Producer
Big Data 07 — NYC Taxi Real-Time Streaming Pipeline

Reads taxi trip records from PostgreSQL ordered by pickup_datetime
and publishes each record as a JSON message to Kafka topic 'taxi-trips'
at a controlled rate of 10 records/second.
"""

import os
import json
import time
import logging
from typing import Optional, Generator, Any
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# ── Load environment variables from .env ──────────────────────
from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

# ── Logging configuration ──────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────
KAFKA_BROKER: str   = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC: str    = os.getenv("KAFKA_TOPIC", "taxi-trips")
PUBLISH_RATE: float = 0.1   # seconds between messages = 10 records/sec
BATCH_SIZE: int     = 500   # fetch rows from PostgreSQL in batches


def get_postgres_connection() -> psycopg2.extensions.connection:
    """
    Establish and return a PostgreSQL connection using .env credentials.
    Raises an exception with a clear message if connection fails.
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=5432,
            dbname=os.getenv("POSTGRES_DB", "taxidb"),
            user=os.getenv("POSTGRES_USER", "taxiuser"),
            password=os.getenv("POSTGRES_PASSWORD", "taxipass")
        )
        logger.info("✅ PostgreSQL connection established")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"❌ PostgreSQL connection failed: {e}")
        raise


def get_kafka_producer() -> KafkaProducer:
    """
    Create and return a KafkaProducer with JSON serialization.
    Retries up to 5 times with 3-second delay between attempts.

    Why JSON serialization?
    Kafka messages are raw bytes. We serialize Python dicts to JSON
    bytes so the consumer can deserialize them back to dicts easily.
    """
    retries: int = 5
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                acks="all",           # wait for all replicas to acknowledge
                retries=3,            # retry failed sends up to 3 times
                linger_ms=10,         # batch messages for 10ms before sending
                request_timeout_ms=30000
            )
            logger.info(f"✅ Kafka producer connected to {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"⚠️  Kafka not ready — attempt {attempt}/{retries}. Retrying in 3s...")
            time.sleep(3)
    raise RuntimeError(f"❌ Could not connect to Kafka after {retries} attempts")


def fetch_trips(
    conn: psycopg2.extensions.connection,
    batch_size: int = BATCH_SIZE
) -> Generator[dict[str, Any], None, None]:
    """
    Generator that yields taxi trip records one at a time from PostgreSQL.

    Why a generator?
    With 25,000 rows, loading all records into memory at once wastes RAM.
    A generator yields one row at a time — memory usage stays constant
    regardless of dataset size. This is the production-grade approach.

    Why server-side cursor?
    psycopg2.extras.DictCursor returns rows as dict-like objects,
    avoiding manual column index mapping.
    """
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("""
        SELECT
            trip_id,
            pickup_datetime,
            dropoff_datetime,
            passenger_count,
            trip_distance,
            pickup_longitude,
            pickup_latitude,
            dropoff_longitude,
            dropoff_latitude,
            payment_type,
            fare_amount,
            tip_amount,
            tolls_amount,
            total_amount,
            vendor_id
        FROM taxi_trips
        ORDER BY pickup_datetime ASC
    """)

    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        for row in rows:
            yield dict(row)

    cursor.close()


def serialize_record(record: dict[str, Any]) -> Optional[dict[str, Any]]:
    """
    Convert a PostgreSQL row dict into a clean JSON-serializable dict.
    Returns None if the record is malformed — producer skips None records.

    Why explicit validation?
    Malformed rows (null distances, negative fares) would corrupt
    downstream InfluxDB writes. We catch them here at the source.
    """
    try:
        # Validate required numeric fields are present and positive
        trip_distance: float = float(record["trip_distance"] or 0)
        fare_amount: float   = float(record["fare_amount"] or 0)

        if trip_distance <= 0 or fare_amount <= 0:
            logger.warning(f"⚠️  Skipping malformed record trip_id={record['trip_id']}: "
                          f"distance={trip_distance}, fare={fare_amount}")
            return None

        return {
            "trip_id":           int(record["trip_id"]),
            "pickup_datetime":   str(record["pickup_datetime"]),
            "dropoff_datetime":  str(record["dropoff_datetime"]),
            "passenger_count":   int(record["passenger_count"] or 1),
            "trip_distance":     round(trip_distance, 2),
            "pickup_longitude":  float(record["pickup_longitude"] or 0),
            "pickup_latitude":   float(record["pickup_latitude"] or 0),
            "dropoff_longitude": float(record["dropoff_longitude"] or 0),
            "dropoff_latitude":  float(record["dropoff_latitude"] or 0),
            "payment_type":      str(record["payment_type"] or "Unknown"),
            "fare_amount":       round(fare_amount, 2),
            "tip_amount":        round(float(record["tip_amount"] or 0), 2),
            "tolls_amount":      round(float(record["tolls_amount"] or 0), 2),
            "total_amount":      round(float(record["total_amount"] or 0), 2),
            "vendor_id":         str(record["vendor_id"] or "Unknown"),
            "event_timestamp":   datetime.utcnow().isoformat()
        }
    except (TypeError, ValueError, KeyError) as e:
        logger.error(f"❌ Record serialization error: {e} | record={record}")
        return None


def on_send_success(record_metadata: Any) -> None:
    """Callback fired when a message is successfully acknowledged by Kafka."""
    pass  # Silent success — we log counts instead of per-message noise


def on_send_error(exc: Exception) -> None:
    """Callback fired when a message fails to send to Kafka."""
    logger.error(f"❌ Kafka send error: {exc}")


def run_producer() -> None:
    """
    Main producer loop:
    1. Connect to PostgreSQL
    2. Connect to Kafka
    3. Stream records from PostgreSQL → Kafka at controlled rate
    4. Log progress every 100 records
    5. Gracefully handle keyboard interrupt
    """
    logger.info("🚀 Starting NYC Taxi Kafka Producer")
    logger.info(f"   Topic  : {KAFKA_TOPIC}")
    logger.info(f"   Broker : {KAFKA_BROKER}")
    logger.info(f"   Rate   : {1/PUBLISH_RATE:.0f} records/second")

    conn: psycopg2.extensions.connection = get_postgres_connection()
    producer: KafkaProducer              = get_kafka_producer()

    sent_count:    int = 0
    skipped_count: int = 0
    error_count:   int = 0

    try:
        for raw_record in fetch_trips(conn):
            record = serialize_record(raw_record)

            if record is None:
                skipped_count += 1
                continue

            try:
                producer.send(
                    KAFKA_TOPIC,
                    value=record
                ).add_callback(on_send_success).add_errback(on_send_error)

                sent_count += 1

                # Progress log every 100 records
                if sent_count % 100 == 0:
                    logger.info(
                        f"📤 Sent: {sent_count:,} | "
                        f"Skipped: {skipped_count} | "
                        f"Errors: {error_count} | "
                        f"Last trip_id: {record['trip_id']} | "
                        f"Fare: ${record['fare_amount']}"
                    )

                time.sleep(PUBLISH_RATE)

            except KafkaError as e:
                error_count += 1
                logger.error(f"❌ Failed to send trip_id={record['trip_id']}: {e}")
                continue

    except KeyboardInterrupt:
        logger.info("⛔ Producer stopped by user")

    finally:
        producer.flush()   # ensure all buffered messages are sent
        producer.close()
        conn.close()
        logger.info(f"✅ Producer finished — Sent: {sent_count:,} | "
                   f"Skipped: {skipped_count} | Errors: {error_count}")


if __name__ == "__main__":
    run_producer()
