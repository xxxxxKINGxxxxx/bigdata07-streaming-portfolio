FROM apache/spark:3.5.0

USER root

RUN pip install --no-cache-dir redis==5.0.1

ENV KAFKA_BROKER=aqi-kafka:29092
ENV KAFKA_TOPIC=aqi-stream
ENV REDIS_HOST=aqi-redis
ENV REDIS_PORT=6379
ENV WINDOW_DURATION="30 seconds"
ENV WATERMARK_DELAY="10 seconds"
ENV CHECKPOINT_DIR=/tmp/checkpoint

COPY streaming_job.py /opt/spark/work-dir/streaming_job.py

WORKDIR /opt/spark/work-dir

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "--conf", "spark.sql.shuffle.partitions=4", \
     "streaming_job.py"]
