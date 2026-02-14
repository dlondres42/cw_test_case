import os
import sys
import pandas as pd
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode


def load_and_group(csv_path: str) -> list[list[dict]]:
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span(
        "csv load_and_group",
        kind=SpanKind.INTERNAL,
        attributes={
            "csv.path": csv_path,
        }
    ) as span:
        if not os.path.exists(csv_path):
            span.set_status(Status(StatusCode.ERROR, "CSV file not found"))
            print(f"Error: CSV file not found at {csv_path}")
            sys.exit(1)
            
        df = pd.read_csv(csv_path, parse_dates=["timestamp"])
        grouped = df.groupby("timestamp")
        batches = []
        for ts, group in sorted(grouped):
            records = [
                {
                    "timestamp": row["timestamp"].isoformat(),
                    "status": row["status"],
                    "count": int(row["count"]),
                }
                for _, row in group.iterrows()
            ]
            batches.append(records)
        
        span.set_attribute("csv.batches_count", len(batches))
        span.set_attribute("csv.total_records", len(df))
        return batches
