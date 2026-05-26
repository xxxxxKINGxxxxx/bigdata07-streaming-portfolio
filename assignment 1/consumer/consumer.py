"""
Assignment 1 — Task 2 + Task 3: Kafka Consumer with Enrichment + InfluxDB Sink
Big Data 07 — NYC Taxi Real-Time Streaming Pipeline

Reads taxi trip messages from Kafka topic 'taxi-trips',
computes 4 enrichment fields, prints formatted summary,
and writes each enriched record to InfluxDB.
"""

import os
import json
import logging
from typing import Optional, Any
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

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
KAFKA_BROKER: str        = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC: str         = os.getenv("KAFKA_TOPIC", "taxi-trips")
KAFKA_GROUP_ID: str      = "taxi-consumer-group"
INFLUXDB_URL: str        = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN: str      = os.getenv("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN", "mySecretToken123456789")
INFLUXDB_ORG: str        = os.getenv("DOCKER_INFLUXDB_INIT_ORG", "bigdata07")
INFLUXDB_BUCKET: str     = os.getenv("DOCKER_INFLUXDB_INIT_BUCKET", "taxi_stream")
MEASUREMENT_NAME: str    = "taxi_trips"


def get_kafka_consumer() -> KafkaConsumer:
    """
    Create and return a KafkaConsumer subscribed to KAFKA_TOPIC.

    Why consumer groups?
    A consumer group allows multiple consumer instances to share
    the workload of reading from a topic. Kafka tracks which messages
    each group has already consumed via offsets — so if the consumer
    restarts, it resumes from where it left off, not from the beginning.
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",   # start from first message if no offset exists
        enable_auto_commit=True,        # automatically commit offsets every 5 seconds
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=10000       # stop after 10s of no new messages
    )
    logger.info(f"✅ Kafka consumer connected — topic: {KAFKA_TOPIC}")
    return consumer


def get_influxdb_client() -> tuple[InfluxDBClient, Any]:
    """
    Create and return an InfluxDB client and write API.

    Why SYNCHRONOUS write mode?
    In synchronous mode, each write blocks until InfluxDB confirms
    receipt. This guarantees data integrity — we know every record
    that leaves the consumer actually lands in InfluxDB.
    For higher throughput, ASYNCHRONOUS batching would be used instead.
    """
    client = InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG
    )
    write_api = client.write_api(write_options=SYNCHRONOUS)
    logger.info(f"✅ InfluxDB client connected — bucket: {INFLUXDB_BUCKET}")
    return client, write_api


def compute_enrichments(record: dict[str, Any]) -> Optional[dict[str, Any]]:
    """
    Task 2 — Compute 4 meaningful derived fields from raw trip data.

    Enrichment fields:
    ┌─────────────────────┬──────────────────────────────────────────────┐
    │ fare_per_mile       │ Cost efficiency — how much per mile traveled │
    │ trip_duration_mins  │ Actual trip length in minutes                │
    │ speed_mph           │ Average speed — reveals traffic conditions   │
    │ fare_category       │ Classification: Low/Medium/High/Premium      │
    └─────────────────────┴──────────────────────────────────────────────┘

    Why these fields?
    - fare_per_mile: identifies price gouging or unusual routes
    - trip_duration_mins: enables time-based analysis
    - speed_mph: low speed = heavy traffic; high speed = highway/night
    - fare_category: turns a continuous variable into a categorical
      dimension, enabling grouping in Grafana panels
    """
    try:
        trip_distance: float = float(record["trip_distance"])
        fare_amount: float   = float(record["fare_amount"])

        # Parse pickup and dropoff datetimes
        pickup_dt  = datetime.fromisoformat(record["pickup_datetime"])
        dropoff_dt = datetime.fromisoformat(record["dropoff_datetime"])

        # trip_duration_mins: total seconds divided by 60
        duration_secs: float  = (dropoff_dt - pickup_dt).total_seconds()
        trip_duration_mins: float = round(max(duration_secs / 60.0, 0.1), 2)

        # fare_per_mile: primary enrichment field — cost efficiency metric
        fare_per_mile: float = round(fare_amount / trip_distance, 2)

        # speed_mph: distance divided by time in hours
        # Capped at 80 mph — anything above is a data error
        speed_mph: float = round(
            min((trip_distance / (trip_duration_mins / 60.0)), 80.0), 2
        )

        # fare_category: classification based on total fare amount
        if fare_amount < 10.0:
            fare_category: str = "Low"
        elif fare_amount < 25.0:
            fare_category = "Medium"
        elif fare_amount < 50.0:
            fare_category = "High"
        else:
            fare_category = "Premium"

        return {
            **record,
            "fare_per_mile":       fare_per_mile,
            "trip_duration_mins":  trip_duration_mins,
            "speed_mph":           speed_mph,
            "fare_category":       fare_category,
        }

    except (TypeError, ValueError, ZeroDivisionError) as e:
        logger.error(f"❌ Enrichment error for trip_id={record.get('trip_id')}: {e}")
        return None


def print_enriched_record(record: dict[str, Any], count: int) -> None:
    """
    Task 2 — Print a formatted summary of each enriched record.
    Prints every record for first 5, then every 50th to avoid terminal flood.
    """
    if count > 5 and count % 50 != 0:
        return

    print("\n" + "─" * 60)
    print(f"  🚕 TAXI TRIP #{count:,}  |  trip_id: {record['trip_id']}")
    print("─" * 60)
    print(f"  📅 Pickup   : {record['pickup_datetime']}")
    print(f"  📍 Route    : ({record['pickup_latitude']:.4f}, {record['pickup_longitude']:.4f})")
    print(f"                → ({record['dropoff_latitude']:.4f}, {record['dropoff_longitude']:.4f})")
    print(f"  👥 Passengers: {record['passenger_count']}")
    print(f"  🛣️  Distance  : {record['trip_distance']} miles")
    print(f"  ⏱️  Duration  : {record['trip_duration_mins']} mins")
    print(f"  🚀 Speed     : {record['speed_mph']} mph")
    print(f"  💳 Payment   : {record['payment_type']}")
    print(f"  💰 Fare      : ${record['fare_amount']}")
    print(f"  🏷️  Category  : {record['fare_category']}")
    print(f"  📊 $/mile    : ${record['fare_per_mile']}  ← ENRICHED FIELD")
    print("─" * 60)


def build_influxdb_point(record: dict[str, Any]) -> Point:
    """
    Task 3 — Build an InfluxDB Point from an enriched record.

    InfluxDB data model:
    ┌──────────────┬────────────────────────────────────────────────┐
    │ measurement  │ The 'table' name — taxi_trips                  │
    │ tags         │ Indexed string columns — for filtering/grouping│
    │ fields       │ Numeric/string values — the actual measurements│
    │ timestamp    │ The time axis — pickup_datetime                │
    └──────────────┴────────────────────────────────────────────────┘

    Why tags vs fields?
    Tags are indexed by InfluxDB — queries that filter or group by
    payment_type or vendor_id are fast because InfluxDB builds an
    inverted index on tags, just like a database index on a column.
    Fields are NOT indexed — they store the actual measurements.
    Putting high-cardinality data (like trip_id) in tags would bloat
    the index and degrade performance.
    """
    pickup_dt = datetime.fromisoformat(record["pickup_datetime"])

    point = (
        Point(MEASUREMENT_NAME)
        # ── TAGS (categorical — indexed for fast filtering) ──
        .tag("payment_type",  record["payment_type"])
        .tag("vendor_id",     record["vendor_id"])
        .tag("fare_category", record["fare_category"])
        # ── FIELDS (numeric measurements) ────────────────────
        .field("trip_id",            record["trip_id"])
        .field("passenger_count",    record["passenger_count"])
        .field("trip_distance",      record["trip_distance"])
        .field("trip_duration_mins", record["trip_duration_mins"])
        .field("speed_mph",          record["speed_mph"])
        .field("fare_amount",        record["fare_amount"])
        .field("tip_amount",         record["tip_amount"])
        .field("tolls_amount",       record["tolls_amount"])
        .field("total_amount",       record["total_amount"])
        .field("fare_per_mile",      record["fare_per_mile"])
        .field("pickup_latitude",    record["pickup_latitude"])
        .field("pickup_longitude",   record["pickup_longitude"])
        .field("dropoff_latitude",   record["dropoff_latitude"])
        .field("dropoff_longitude",  record["dropoff_longitude"])
        # ── TIMESTAMP ─────────────────────────────────────────
        .time(pickup_dt)
    )
    return point


def write_to_influxdb(
    write_api: Any,
    point: Point,
    trip_id: int
) -> bool:
    """
    Write a single InfluxDB point to the bucket.
    Returns True on success, False on failure.
    """
    try:
        write_api.write(
            bucket=INFLUXDB_BUCKET,
            org=INFLUXDB_ORG,
            record=point
        )
        return True
    except Exception as e:
        logger.error(f"❌ InfluxDB write failed for trip_id={trip_id}: {e}")
        return False


def run_consumer() -> None:
    """
    Main consumer loop:
    1. Connect to Kafka and InfluxDB
    2. Read messages from Kafka topic
    3. Compute enrichment fields (Task 2)
    4. Print formatted summary (Task 2)
    5. Write to InfluxDB (Task 3)
    6. Log progress every 100 records
    """
    logger.info("🚀 Starting NYC Taxi Kafka Consumer")
    logger.info(f"   Topic   : {KAFKA_TOPIC}")
    logger.info(f"   Group   : {KAFKA_GROUP_ID}")
    logger.info(f"   InfluxDB: {INFLUXDB_URL}/{INFLUXDB_BUCKET}")

    consumer                          = get_kafka_consumer()
    influx_client, write_api          = get_influxdb_client()

    consumed_count:  int = 0
    enriched_count:  int = 0
    influx_ok_count: int = 0
    influx_err_count: int = 0

    try:
        for message in consumer:
            try:
                raw_record: dict[str, Any] = message.value
                consumed_count += 1

                # ── Task 2: Enrich record ──────────────────────
                enriched = compute_enrichments(raw_record)
                if enriched is None:
                    continue
                enriched_count += 1

                # ── Task 2: Print formatted summary ───────────
                print_enriched_record(enriched, consumed_count)

                # ── Task 3: Write to InfluxDB ──────────────────
                point = build_influxdb_point(enriched)
                success = write_to_influxdb(write_api, point, enriched["trip_id"])

                if success:
                    influx_ok_count += 1
                else:
                    influx_err_count += 1

                # Progress log every 100 records
                if consumed_count % 100 == 0:
                    logger.info(
                        f"📥 Consumed: {consumed_count:,} | "
                        f"Enriched: {enriched_count:,} | "
                        f"InfluxDB OK: {influx_ok_count:,} | "
                        f"InfluxDB Err: {influx_err_count}"
                    )

            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"❌ Message processing error: {e}")
                continue

    except KeyboardInterrupt:
        logger.info("⛔ Consumer stopped by user")

    except KafkaError as e:
        logger.error(f"❌ Kafka error: {e}")

    finally:
        consumer.close()
        write_api.close()
        influx_client.close()
        logger.info(
            f"✅ Consumer finished — "
            f"Consumed: {consumed_count:,} | "
            f"Enriched: {enriched_count:,} | "
            f"InfluxDB writes: {influx_ok_count:,}"
        )


if __name__ == "__main__":
    run_consumer()
