# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## Assignment Context

- **Subject:** Big Data 07 — Practical Assignment Portfolio
- **Assignment:** 1 of 3 (30 marks + 3 bonus)
- **Student directory:** `E:\School\Abdallah Assignment\assignment 1`
- **Terminal:** Claude Code in Antigravity

---

## Scoring Targets

| Task | Marks | Description |
|------|-------|-------------|
| Task 1 — Producer | 6 | PostgreSQL → Kafka, JSON messages, error handling |
| Task 2 — Consumer + Enrichment | 6 | Derived field, formatted output |
| Task 3 — InfluxDB Sink | 5 | Tags vs fields, visible in Data Explorer |
| Task 4 — Grafana Dashboard | 9 | 4+ panels, auto-refresh, titled |
| Task 5 — Analysis | 4 | Dataset-specific answers |
| Bonus — Geomap | +3 | Lat/lon panel in Grafana |
| **TOTAL** | **33** | |

---

## Tech Stack & Pipeline

```
PostgreSQL → Python Producer → Kafka → Python Consumer → InfluxDB → Grafana
```

All services run via Docker Compose.

### Docker Services

| Service | Image | Port |
|---------|-------|------|
| zookeeper | confluentinc/cp-zookeeper:7.5.0 | 2181 |
| kafka | confluentinc/cp-kafka:7.5.0 | 9092 |
| postgres | postgres:15 | 5432 |
| influxdb | influxdb:2.7 | 8086 |
| grafana | grafana/grafana:10.2.0 | 3000 |

---

## Project File Structure

```
assignment 1/
├── CLAUDE.md
├── docker-compose.yml         ← all 5 services
├── .env                       ← all credentials
├── requirements.txt           ← Python dependencies
├── data/
│   └── init.sql               ← PostgreSQL schema + 25,000 taxi rows
├── producer/
│   └── producer.py            ← Task 1
└── consumer/
    └── consumer.py            ← Task 2 + Task 3
```

---

## Common Commands

```powershell
# Start all services
docker compose up -d

# Check all containers are running
docker ps

# View producer logs
docker compose logs -f producer

# View consumer logs
docker compose logs -f consumer

# Stop all services
docker compose down

# Stop and wipe volumes (reset InfluxDB/Postgres data)
docker compose down -v

# Run producer manually (outside Docker)
python producer/producer.py

# Run consumer manually (outside Docker)
python consumer/consumer.py

# Install Python dependencies
pip install -r requirements.txt
```

---

## Environment Variables

All credentials live in `.env` — never hardcoded in source files.

```env
# PostgreSQL
POSTGRES_DB=taxidb
POSTGRES_USER=taxiuser
POSTGRES_PASSWORD=taxipass

# Kafka
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=taxi-trips

# InfluxDB
DOCKER_INFLUXDB_INIT_MODE=setup
DOCKER_INFLUXDB_INIT_USERNAME=admin
DOCKER_INFLUXDB_INIT_PASSWORD=adminpass123
DOCKER_INFLUXDB_INIT_ORG=bigdata07
DOCKER_INFLUXDB_INIT_BUCKET=taxi_stream
DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=mySecretToken123456789

# Grafana
GF_SECURITY_ADMIN_PASSWORD=admin123
```

---

## Dataset

- **Source:** PostgreSQL container (`taxidb`)
- **Table:** `taxi_trips` (25,000 rows)
- **Key columns:** `pickup_datetime`, `dropoff_datetime`, `passenger_count`, `trip_distance`, `pickup_longitude`, `pickup_latitude`, `dropoff_longitude`, `dropoff_latitude`, `payment_type`, `fare_amount`, `tip_amount`, `tolls_amount`, `total_amount`, `vendor_id`

---

## Enrichment Fields (Task 2)

| Field | Formula | Purpose |
|-------|---------|---------|
| `fare_per_mile` | `fare_amount / trip_distance` | Cost efficiency metric |
| `trip_duration_mins` | `(dropoff - pickup).seconds / 60` | Actual trip length |
| `speed_mph` | `trip_distance / (duration / 60)` | Average speed |
| `fare_category` | Low/Medium/High/Premium based on fare | Classification tag |

---

## InfluxDB Schema (Task 3)

- **Tags** (indexed, low-cardinality): `payment_type`, `vendor_id`, `fare_category`
- **Fields** (numeric measurements): `fare_amount`, `tip_amount`, `trip_distance`, `fare_per_mile`, `trip_duration_mins`, `speed_mph`, `passenger_count`
- **Timestamp:** `pickup_datetime`

---

## Grafana Dashboard (Task 4)

| Panel | Type | Metric |
|-------|------|--------|
| 1 | Time Series | `fare_amount` over time |
| 2 | Bar Chart | avg `fare_per_mile` by `payment_type` |
| 3 | Gauge | avg `trip_distance` |
| 4 | Stat | total trips processed |
| 5 (Bonus) | Geomap | pickup coordinates (`pickup_latitude` / `pickup_longitude`) |

Dashboard must have: auto-refresh enabled, title set, all panels titled.

---

## Thinking Level Guide

Use `/think hard` as the default for this project. Escalate to `/ultrathink` only for high-risk steps.

| Situation | Command |
|-----------|---------|
| Config files (`docker-compose.yml`, `.env`, `init.sql`) | *(none — straightforward)* |
| `producer.py` — error handling, type hints, rate limiting | `/think hard` |
| `consumer.py` — enrichment logic, InfluxDB write | `/think hard` |
| InfluxDB tags vs fields decisions | `/think hard` |
| Grafana Flux queries | `/ultrathink` |
| Task 5 analysis answers | `/think hard` |

---

## Strict Coding Rules

1. **Teach WHY, WHAT, HOW** at every step — explain like a professor, not just deliver code.
2. **Always choose the highest-scoring option** — when there's a choice, pick what earns more marks.
3. **Full code only** — no placeholders, no `# ... rest of code here` truncation.
4. **Strict Python type hints** — always `from typing import ...` and annotate all functions.
5. **Vectorized operations** — use NumPy/Pandas vectorization; avoid manual Python loops over data.
6. **📸 TAKE SCREENSHOT NOW** — remind the student at every moment a screenshot is required.

---

## Required Screenshots (6 total)

1. `docker ps` showing all 5 containers running
2. Producer terminal sending records to Kafka
3. Consumer terminal showing enriched output
4. InfluxDB Data Explorer showing incoming data
5. Grafana dashboard with all panels populated
6. Grafana Geomap panel with NYC coordinates (bonus)
