# 🚕 Big Data 07 — Real-Time Streaming Pipeline Portfolio

![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=flat&logo=grafana&logoColor=white)
![InfluxDB](https://img.shields.io/badge/InfluxDB-22ADF6?style=flat&logo=influxdb&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?style=flat&logo=redis&logoColor=white)

> A complete end-to-end big data engineering portfolio built from scratch.
> Real-time streaming, batch analytics, and continuous stream processing
> using industry-standard tools deployed via Docker.

---

## 👤 Author
**Hayagriva Boodhoo** | Student ID: 2025_20379_6980 | Cohort: BDA 7

---

## 🏗️ Full Pipeline Architecture

Assignment 1 — Real-Time Streaming:
PostgreSQL → Kafka Producer → Kafka Topic → Consumer + Enrichment → InfluxDB → Grafana
Assignment 2 — Batch Analytics:
CSV Dataset → PySpark (Clean + Aggregate) → Snowflake → Streamlit Dashboard
Assignment 3 — Continuous Stream Processing:
Kafka Replay Producer → Kafka Topic → PySpark Structured Streaming → Redis → Streamlit Live Dashboard


---

## 📁 Assignment Breakdown

### Assignment 1 — Apache Kafka + Grafana (30 Marks)
Real-time data pipeline streaming NYC Taxi trip data through Kafka into InfluxDB with live Grafana visualisation.

**Technologies:** Apache Kafka · PostgreSQL · InfluxDB 2.x · Grafana · Python · Docker Compose

**What it does:**
- Python producer reads 25,000 NYC taxi trips from PostgreSQL and publishes to Kafka at 10 records/sec
- Consumer enriches each record with 4 derived fields: fare_per_mile, trip_duration_mins, speed_mph, fare_category
- Enriched records written to InfluxDB with correct tags/fields structure
- Grafana dashboard with 5 panels (Time Series, Bar Chart, Gauge, Stat, Geomap) auto-refreshes every 30s

**Key concepts demonstrated:**
- Event-driven architecture with producer/consumer decoupling
- Time-series database vs relational database for streaming data
- Kafka offset management and consumer groups
- InfluxDB tags vs fields data model
- Docker Compose multi-service orchestration

---

### Assignment 2 — Apache Spark + Snowflake (30 Marks)
Batch analytics pipeline processing a large dataset with PySpark and exposing insights via an interactive Streamlit dashboard backed by Snowflake.

**Technologies:** PySpark · Snowflake · Python · Streamlit · Docker

**What it does:**
- PySpark loads and cleans dataset with type casting and derived columns
- 4 business questions answered using both DataFrame API and Spark SQL
- Window functions: ranking, cumulative totals, period-over-period growth
- Data loaded into Snowflake cloud data warehouse via Python connector
- Interactive Streamlit dashboard queries Snowflake with reactive filters

**Key concepts demonstrated:**
- PySpark DataFrame API vs Spark SQL
- Window functions (rank, sum over partition, lag/lead)
- Snowflake cloud data warehouse architecture
- Streamlit reactive UI with filter controls

---

### Assignment 3 — PySpark Structured Streaming (30 Marks)
Continuous stream processing pipeline that replays a dataset as a live Kafka stream, processes it with PySpark Structured Streaming, and visualises results in a real-time Streamlit dashboard.

**Technologies:** PySpark Structured Streaming · Kafka · Redis · Streamlit · Plotly · Docker

**What it does:**
- Kafka replay producer streams dataset rows continuously, looping back to start
- PySpark Structured Streaming reads from Kafka in micro-batches
- Tumbling and sliding time windows with per-window aggregations
- Classification bands applied to key numeric indicator (severity levels)
- Watermarking handles late-arriving events
- Results written to Redis for low-latency dashboard consumption
- Streamlit dashboard auto-refreshes with KPIs, charts, classification breakdown

**Key concepts demonstrated:**
- Batch vs streaming processing comparison
- Tumbling windows vs sliding windows
- Watermarking and late data handling
- writeStream with foreachBatch sink
- Redis as in-memory streaming sink

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop installed and running
- Python 3.12+
- Git

### Clone the repo
```bash
git clone https://github.com/xxxxxKINGxxxxx/bigdata07-streaming-portfolio.git
cd bigdata07-streaming-portfolio
```

### Run Assignment 1
```bash
cd "assignment 1"
cp .env.example .env
# Edit .env with your credentials
docker compose up -d
docker logs producer -f
```

Open Grafana: http://localhost:3000 (admin / admin123)
Open InfluxDB: http://localhost:8086 (admin / adminpass123)

### Run Assignment 2
```bash
cd "assignment 2"
cp .env.example .env
# Edit .env with your Snowflake credentials
docker compose up -d
```

Open Streamlit: http://localhost:8501

### Run Assignment 3
```bash
cd "assignment 3"
cp .env.example .env
docker compose up -d
docker logs producer -f
```

Open Streamlit: http://localhost:8501

---

## 📂 Project Structure


bigdata07-streaming-portfolio/
├── README.md
├── .gitignore
├── .env.example
├── assignment 1/
│   ├── docker-compose.yml
│   ├── .env.example
│   ├── requirements.txt
│   ├── data/init.sql
│   ├── producer/producer.py
│   └── consumer/consumer.py
├── assignment 2/
│   ├── docker-compose.yml
│   ├── .env.example
│   ├── requirements.txt
│   ├── partA/
│   ├── partB/
│   └── partC/
└── assignment 3/
├── docker-compose.yml
├── .env.example
├── producer/producer.py
├── spark/streaming_job.py
└── dashboard/dashboard.py


---

## 🛠️ Technologies Used

| Category | Technologies |
|----------|-------------|
| Streaming | Apache Kafka, PySpark Structured Streaming |
| Databases | PostgreSQL, InfluxDB 2.x, Snowflake, Redis |
| Processing | Apache Spark (PySpark), Python |
| Visualisation | Grafana, Streamlit, Plotly |
| Infrastructure | Docker, Docker Compose |
| Languages | Python 3.12 |

---

## 📄 License
MIT License — feel free to use this as a reference for your own big data projects.

