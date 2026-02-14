FROM python:3.11-slim

WORKDIR /app

# Copy source code
COPY toy_project/common ./common
COPY toy_project/stream_processor ./stream_processor

# Install dependencies
RUN pip install --no-cache-dir ./common
RUN pip install --no-cache-dir ./stream_processor

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV KAFKA_TOPIC=transactions
ENV CSV_PATH=/data/sample_data/transactions/transactions.csv
ENV STREAM_DELAY=0.1

# Run as module
CMD ["python", "-m", "stream_processor.main"]
