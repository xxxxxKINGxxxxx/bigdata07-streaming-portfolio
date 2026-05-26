import csv
import json
import time
import os
import logging
from typing import Generator
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ── Logging setup ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ── Config from environment ─────────────────────────────────────
KAFKA_BROKER: str = os.getenv("KAFKA_BROKER", "aqi-kafka:29092")
TOPIC: str        = os.getenv("KAFKA_TOPIC",  "aqi-stream")
DELAY: float      = float(os.getenv("PUBLISH_DELAY", "0.1"))
CSV_PATH: str     = os.getenv("CSV_PATH", "/data/US_AQI.csv")

# ── AQI classification bands ────────────────────────────────────
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

# ── Row reader with loop ────────────────────────────────────────
def stream_csv(path: str) -> Generator[dict, None, None]:
    loop_count: int = 0
    while True:
        loop_count += 1
        log.info(f"Starting loop #{loop_count} through dataset")
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row

# ── Build message payload ───────────────────────────────────────
def build_message(row: dict) -> dict | None:
    try:
        aqi_value: float = float(row["AQI"])
        message: dict = {
            "state":      row.get("state_name", "Unknown").strip(),
            "city":       row.get("city_ascii", "Unknown").strip(),
            "date":       row.get("Date", "").strip(),
            "aqi":        aqi_value,
            "category":   row.get("Category", classify_aqi(aqi_value)).strip(),
            "band":       classify_aqi(aqi_value),
            "parameter":  row.get("Defining Parameter", "Unknown").strip(),
            "lat":        float(row.get("lat", 0) or 0),
            "lng":        float(row.get("lng", 0) or 0),
            "population": float(row.get("population", 0) or 0),
            "timestamp":  time.time()
        }
        return message
    except (ValueError, KeyError) as e:
        log.warning(f"Skipping malformed row: {e} — row={row}")
        return None

# ── Main ────────────────────────────────────────────────────────
def main() -> None:
    log.info(f"Connecting to Kafka broker at {KAFKA_BROKER}")
    log.info(f"Publishing to topic: {TOPIC}")
    log.info(f"Delay between messages: {DELAY}s")

    producer: KafkaProducer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        retry_backoff_ms=1000
    )

    sent: int = 0
    skipped: int = 0

    for row in stream_csv(CSV_PATH):
        message = build_message(row)
        if message is None:
            skipped += 1
            continue
        try:
            producer.send(TOPIC, value=message)
            sent += 1
            if sent % 100 == 0:
                producer.flush()
                log.info(
                    f"Sent: {sent:,} | Skipped: {skipped} | "
                    f"Last → {message['state']} / {message['city']} "
                    f"AQI={message['aqi']} Band={message['band']}"
                )
            time.sleep(DELAY)
        except KafkaError as e:
            log.error(f"Kafka send error: {e}")
            time.sleep(2)

if __name__ == "__main__":
    main()
