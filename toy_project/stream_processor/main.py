import argparse
import os
import threading
import sys
import time
from prometheus_client import start_http_server

from cw_common.observability import init_observability, get_logger

# Import from local modules
from .loader import load_and_group
from .producer import stream_loop
from .controls import paused, stop_event
from . import telemetry

# Bootstrap logging + tracing + service-info in one call
init_observability("stream-processor", "0.2.0")

logger = get_logger("stream-processor")


def main():
    # Start Prometheus metrics endpoint
    metrics_port = int(os.environ.get("METRICS_PORT", "8001"))
    try:
        start_http_server(metrics_port)
        logger.info(f"Prometheus metrics server started on port {metrics_port}")
    except Exception as e:
        logger.warning(f"Failed to start metrics server: {e}")
    
    parser = argparse.ArgumentParser(description="Mock stream processor - Kafka producer")
    parser.add_argument(
        "--csv",
        default=os.environ.get("CSV_PATH", "../../sample_data/transactions/transactions.csv"),
        help="Path to the transactions CSV file",
    )
    parser.add_argument(
        "--brokers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--topic",
        default=os.environ.get("KAFKA_TOPIC", "transactions"),
        help="Kafka topic to produce to",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=float(os.environ.get("STREAM_DELAY", "0.1")),
        help="Seconds to wait between batches",
    )
    args = parser.parse_args()

    logger.info(f"Loading CSV: {args.csv}")
    batches = load_and_group(args.csv)
    logger.info(f"Loaded {len(batches)} timestamp batches")
    logger.info(f"Producing to: {args.brokers} topic='{args.topic}' (delay: {args.delay}s)")
    logger.info("-" * 60)
    logger.info("CONTROLS: [p]ause, [r]esume, [q]uit")
    logger.info("-" * 60)

    # Start streaming in a background thread
    t = threading.Thread(target=stream_loop, args=(batches, args.brokers, args.topic, args.delay))
    t.start()

    # Main thread handles user input
    try:
        while t.is_alive():
            try:
                # Use blocking input
                cmd = input().strip().lower()
                if cmd == 'p':
                    paused.clear()
                    logger.info("PAUSED")
                elif cmd == 'r':
                    paused.set()
                    logger.info("RESUMED")
                elif cmd == 'q':
                    stop_event.set()
                    paused.set() # Ensure thread unblocks to exit
                    logger.info("Quitting...")
                    break
            except EOFError:
                # Handle EOF (e.g. running detached / no TTY)
                # In this case, we just wait for the thread to finish or keep running
                # But if we break, we exit main, which joins the thread.
                # If running detached in background, we usually want it to just run.
                # So we should sleep?
                # But if stream finishes, loop exits.
                # Let's just wait for thread.
                time.sleep(1)
    except KeyboardInterrupt:
        stop_event.set()
        paused.set()
    
    t.join()
    logger.info("Exited.")


if __name__ == "__main__":
    main()
