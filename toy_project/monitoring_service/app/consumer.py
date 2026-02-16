import os
import json
import threading
import logging
import time

from confluent_kafka import Consumer, KafkaError
from opentelemetry import trace, context
from opentelemetry.trace import SpanKind

from app.database import insert_transactions, get_status_counts_at, get_history_window

from cw_common.observability import extract_trace_context, kafka_headers_to_dict

logger = logging.getLogger("consumer")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "transactions")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "monitoring-service")

# Metrics placeholders — set by telemetry module if available
messages_consumed_counter = None
consume_duration_histogram = None
insert_duration_histogram = None
transactions_by_status_counter = None
transaction_status_rate_gauge = None

# Alert dispatcher — set by telemetry module if available
alert_dispatcher = None
anomaly_detector = None


def _consume_loop(stop_event: threading.Event):
    tracer = trace.get_tracer(__name__)
    # Propagator replaced by common helper
    
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([KAFKA_TOPIC])
    logger.info(f"Subscribed to topic '{KAFKA_TOPIC}' on {KAFKA_BOOTSTRAP_SERVERS}")

    while not stop_event.is_set():
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            logger.error(f"Kafka error: {msg.error()}")
            continue

        start_time = time.monotonic()
        
        # Extract trace context from Kafka headers to continue the distributed trace
        headers_dict = kafka_headers_to_dict(msg.headers())
        parent_ctx = extract_trace_context(headers_dict)
        
        try:
            # Start a consumer span as a child of the producer span (same trace)
            with tracer.start_as_current_span(
                f"{KAFKA_TOPIC} receive",
                context=parent_ctx,
                kind=SpanKind.CONSUMER,
                attributes={
                    "messaging.system": "kafka",
                    "messaging.source.name": KAFKA_TOPIC,
                    "messaging.operation.name": "receive",
                    "messaging.kafka.partition": msg.partition(),
                    "messaging.kafka.offset": msg.offset(),
                    "messaging.kafka.consumer.group": KAFKA_GROUP_ID,
                }
            ) as span:
                payload = json.loads(msg.value().decode("utf-8"))
                records = payload.get("records", [])
                if records:
                    span.set_attribute("batch.size", len(records))
                    span.set_attribute("batch.timestamp", records[0].get('timestamp', '?'))
                    
                    insert_start = time.monotonic()
                    count = insert_transactions(records)
                    insert_elapsed = time.monotonic() - insert_start
                    elapsed = time.monotonic() - start_time

                    logger.info(
                        f"Consumed {count} records "
                        f"(ts: {records[0].get('timestamp', '?')}, "
                        f"partition: {msg.partition()}, "
                        f"offset: {msg.offset()}, "
                        f"latency: {elapsed:.3f}s)"
                    )

                    if messages_consumed_counter:
                        messages_consumed_counter.inc(count)
                    if consume_duration_histogram:
                        consume_duration_histogram.observe(elapsed)
                    if insert_duration_histogram:
                        insert_duration_histogram.observe(insert_elapsed)

                    # Per-status business metrics
                    if transactions_by_status_counter or transaction_status_rate_gauge:
                        for rec in records:
                            status = rec.get("status", "unknown")
                            cnt = rec.get("count", 0)
                            if transactions_by_status_counter:
                                transactions_by_status_counter.labels(status=status).inc(cnt)
                            if transaction_status_rate_gauge:
                                transaction_status_rate_gauge.labels(status=status).set(cnt)

                    # Automatic anomaly detection after each batch
                    if anomaly_detector and alert_dispatcher:
                        try:
                            current_counts = get_status_counts_at(minutes=1)
                            history = get_history_window(minutes=60)
                            if current_counts and history:
                                det_result = anomaly_detector.detect(current_counts, history)
                                if det_result.severity != "NORMAL":
                                    alert_dispatcher.dispatch(det_result)
                        except Exception as exc:
                            logger.debug("Anomaly detection in consumer failed: %s", exc)

        except Exception:
            logger.exception("Failed to process Kafka message")

    consumer.close()
    logger.info("Kafka consumer stopped")


def start_consumer() -> tuple[threading.Thread, threading.Event]:
    stop_event = threading.Event()
    thread = threading.Thread(target=_consume_loop, args=(stop_event,), daemon=True)
    thread.start()
    logger.info("Kafka consumer thread started")
    return thread, stop_event
