# Assignment 3 — PySpark Structured Streaming + Redis + Streamlit

## Student
- Name: Hayagriva Boodhoo
- ID: 2025_20379_6980
- Cohort: BDA 7

## Dataset
- File: data/daily_aqi_by_county_2023.csv
- Rows: ~50,000
- Key column: AQI (numeric, 0–500)
- Classification bands: Good(0-50), Moderate(51-100), Unhealthy for Sensitive(101-150), Unhealthy(151-200), Very Unhealthy(201-300), Hazardous(301-500)

## Pipeline
Kafka Replay Producer → Kafka Topic: aqi-stream → PySpark Structured Streaming → Redis → Streamlit Dashboard

## Tech Stack
- Kafka + Zookeeper: confluentinc/cp-kafka:7.5.0
- Redis: redis:7.2-alpine
- PySpark: bitnami/spark:3.5.0
- Producer: python:3.12-slim
- Dashboard: python:3.12-slim

## Project Structure
- data/         → CSV dataset
- producer/     → Kafka replay producer
- spark/        → PySpark structured streaming job
- dashboard/    → Streamlit live dashboard
- redis/        → Redis config
- docker/       → docker-compose.yml

## Rules
- All scripts run inside Docker containers
- Credentials in .env only
- No pandas/numpy (Python 3.14 host)
- Use kafka-python-ng==2.2.3

## Screenshots Needed
1. docker ps — Kafka, Zookeeper, Redis all running
2. Producer terminal — rows publishing continuously
3. PySpark terminal — micro-batch progress logs
4. Redis CLI — windowed results stored
5. PySpark terminal — watermark applied
6. Full Streamlit dashboard — live data, URL visible
7. Dashboard updated — new windows visible
8. Bonus — sliding vs tumbling window comparison

## Teaching Moments
At every step of this project, explain:
- WHAT we are doing
- WHY we are doing it this way
- HOW it works underneath
- What would break if we did it differently

This is not just a build guide — it is a teaching document.
Every concept must be explained like a professor teaching a student for the first time.
