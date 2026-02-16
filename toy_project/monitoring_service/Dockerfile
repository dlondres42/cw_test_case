FROM python:3.11-slim

WORKDIR /service

COPY toy_project/monitoring_service/pyproject.toml .

# Copy and install common library
COPY toy_project/common /common
RUN pip install --no-cache-dir /common

# Copy and install anomaly model
COPY toy_project/anomaly_model /anomaly_model
RUN pip install --no-cache-dir /anomaly_model

RUN pip install --no-cache-dir .

COPY toy_project/monitoring_service/app/ app/

ENV DATABASE_PATH=/data/transactions.db
ENV DB_RESET_ON_START=true

VOLUME ["/data"]

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
