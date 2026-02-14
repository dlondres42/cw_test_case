import json
import time
import logging
from confluent_kafka import Producer
from opentelemetry import trace
from opentelemetry.trace import SpanKind

from cw_common.observability import (
    inject_trace_context, 
    dict_to_kafka_headers,
    shutdown_tracing
)
from .controls import paused, stop_event
from . import telemetry

logger = logging.getLogger(__name__)


def delivery_callback(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")


def stream_loop(batches: list[list[dict]], brokers: str, topic: str, delay: float):
    tracer = trace.get_tracer(__name__)
    producer = Producer({"bootstrap.servers": brokers})
    total = len(batches)
    i = 0

    logger.info(f"Starting stream... ({total} batches)")

    with tracer.start_as_current_span(
        "stream session",
        kind=SpanKind.INTERNAL,
        attributes={
            "messaging.system": "kafka",
            "messaging.destination": topic,
            "stream.total_batches": total,
        }
    ):
        while i < total and not stop_event.is_set():
            if not paused.is_set():
                time.sleep(0.5)
                continue

            batch = batches[i]
            ts_label = batch[0]["timestamp"]
            payload = json.dumps({"records": batch}).encode("utf-8")

            # Create a span for this produce operation
            produce_start = time.time()
            with tracer.start_as_current_span(
                f"{topic} publish",
                kind=SpanKind.PRODUCER,
                attributes={
                    "messaging.system": "kafka",
                    "messaging.destination.name": topic,
                    "messaging.operation.name": "publish",
                    "messaging.batch.message_count": len(batch),
                    "messaging.batch.timestamp": ts_label,
                }
            ) as span:
                # Inject trace context into Kafka headers
                headers = {}
                inject_trace_context(headers)
                
                # Convert dict to list of tuples for confluent-kafka
                kafka_headers = dict_to_kafka_headers(headers)
                
                producer.produce(topic, value=payload, headers=kafka_headers, callback=delivery_callback)
                # Poll to handle delivery reports
                producer.poll(0)
            
            # Record metrics
            produce_duration = time.time() - produce_start
            telemetry.BATCHES_PRODUCED.inc()
            telemetry.RECORDS_PRODUCED.inc(len(batch))
            telemetry.PRODUCE_DURATION.observe(produce_duration)

            logger.info(f"[{i+1}/{total}] {ts_label} - {len(batch)} records -> produced")
            
            i += 1
            time.sleep(delay)

    # Flush remaining messages
    producer.flush(timeout=10)
    
    if stop_event.is_set():
        logger.info("Stream stopped by user.")
    else:
        logger.info("All messages flushed to Kafka.")
    
    # Shutdown tracer to flush remaining spans
    shutdown_tracing()
    
    logger.info("Press Enter to exit...")
