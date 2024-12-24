import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple

import apache_beam as beam
from apache_beam import window
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.transforms import trigger

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ParseJsonDoFn(beam.DoFn):
    """Parse JSON messages from PubSub."""

    def process(self, element):
        try:
            # Parse JSON message
            record = json.loads(element.decode("utf-8"))

            # Create a unique key for deduplication
            unique_key = f"{record['symbol']}_{record['timestamp']}"

            # Yield key-value pair
            yield (unique_key, record)
        except Exception as e:
            logger.error(f"Error parsing message: {e}")


class DedupRecordsFn(beam.DoFn):
    """Deduplicate records within each window."""

    def process(self, element: Tuple[str, List[Dict]]):
        key, records = element
        # Take the most recent record in case of duplicates
        latest_record = sorted(
            records,
            key=lambda x: datetime.strptime(x["timestamp"], "%Y-%m-%d %H:%M:%S"),
            reverse=True,
        )[0]
        yield latest_record


def run_dedup_pipeline():
    """Run the deduplication pipeline."""
    pipeline_options = PipelineOptions(
        [
            "--project=stock-data-pipeline-444011",
            "--region=us-central1",
            "--streaming",
            "--runner=DataflowRunner",
        ]
    )
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # BigQuery table schema
    table_schema = {
        "fields": [
            {"name": "timestamp", "type": "TIMESTAMP"},
            {"name": "symbol", "type": "STRING"},
            {"name": "open", "type": "FLOAT"},
            {"name": "high", "type": "FLOAT"},
            {"name": "low", "type": "FLOAT"},
            {"name": "close", "type": "FLOAT"},
            {"name": "volume", "type": "INTEGER"},
            {"name": "date", "type": "STRING"},
            {"name": "time", "type": "STRING"},
            {"name": "moving_average", "type": "FLOAT"},
            {"name": "cumulative_average", "type": "FLOAT"},
        ]
    }

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read from PubSub
        messages = (
            pipeline
            | "Read from PubSub"
            >> ReadFromPubSub(
                subscription="projects/stock-data-pipeline-444011/subscriptions/stock-data-sub"
            )
            | "Parse JSON" >> beam.ParDo(ParseJsonDoFn())
            # Apply windowing - 5-minute fixed windows
            | "Window"
            >> beam.WindowInto(
                window.FixedWindows(300),  # 5 minutes
                trigger=trigger.Repeatedly(trigger.AfterCount(1)),
                accumulation_mode=trigger.AccumulationMode.DISCARDING,
            )
            # Group by key for deduplication
            | "Group by Key" >> beam.GroupByKey()
            # Deduplicate within windows
            | "Deduplicate" >> beam.ParDo(DedupRecordsFn())
        )

        # Write to BigQuery
        _ = messages | "Write to BigQuery" >> WriteToBigQuery(
            table="stock_market.stock_data_dedup",
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )


if __name__ == "__main__":
    run_dedup_pipeline()
