FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

ENV KAFKA_BROKER=aqi-kafka:29092
ENV KAFKA_TOPIC=aqi-stream
ENV PUBLISH_DELAY=0.1
ENV CSV_PATH=/data/US_AQI.csv

CMD ["python", "producer.py"]
