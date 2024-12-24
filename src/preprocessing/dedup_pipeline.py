import json
import logging
from datetime import datetime
from typing import Any, Dict, Iterable, Tuple

import apache_beam as beam
from apache_beam import window
from apache_beam.io import ReadFromBigQuery, ReadFromPubSub, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.transforms import trigger
from google.cloud import bigquery

# Import your existing configurations
from config import GCP_CONFIG, STOCK_CONFIGS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_historical_data():
    """Clean up existing data in BigQuery tables."""
    client = bigquery.Client()

    for symbol, config in STOCK_CONFIGS.items():
        table_id = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}.{config['table_name']}"

        # Query to remove duplicates, keeping the latest record for each timestamp
        dedup_query = f"""
        CREATE OR REPLACE TABLE `{table_id}` AS
        WITH ranked_records AS (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol, timestamp
                    ORDER BY moving_average DESC, cumulative_average DESC
                ) as rank
            FROM `{table_id}`
        )
        SELECT 
            timestamp,
            symbol,
            open,
            high,
            low,
            close,
            volume,
            date,
            time,
            moving_average,
            cumulative_average
        FROM ranked_records
        WHERE rank = 1
        ORDER BY timestamp DESC
        """

        try:
            logger.info(f"Cleaning historical data for {symbol}...")
            query_job = client.query(dedup_query)
            query_job.result()
            logger.info(f"Successfully cleaned historical data for {symbol}")
        except Exception as e:
            logger.error(f"Error cleaning historical data for {symbol}: {e}")


class ParseJsonDoFn(beam.DoFn):
    """Parse JSON messages from PubSub."""

    def process(self, element: bytes):
        try:
            record = json.loads(element.decode("utf-8"))
            unique_key = f"{record['symbol']}_{record['timestamp']}"
            yield (unique_key, record)
        except Exception as e:
            logger.error(f"Error parsing message: {e}")


class DedupRecordsFn(beam.DoFn):
    """Deduplicate records within each window."""

    def process(self, element: Tuple[str, Iterable[Dict[str, Any]]]):
        try:
            key, records = element
            records_list = list(records)
            if not records_list:
                return

            latest_record = sorted(
                records_list,
                key=lambda x: datetime.strptime(x["timestamp"], "%Y-%m-%d %H:%M:%S"),
                reverse=True,
            )[0]

            yield latest_record
        except Exception as e:
            logger.error(f"Error in deduplication: {e}")


def run_dedup_pipeline():
    """Run the deduplication pipeline."""
    # First, clean historical data
    logger.info("Starting historical data cleanup...")
    clean_historical_data()
    logger.info("Completed historical data cleanup")

    # Now set up streaming pipeline for new data
    pipeline_options = PipelineOptions(
        [
            f'--project={GCP_CONFIG["PROJECT_ID"]}',
            "--region=us-central1",
            "--streaming",
            "--runner=DataflowRunner",
            f'--temp_location=gs://{GCP_CONFIG["BUCKET_NAME"]}/temp',
            "--setup_file=./setup.py",
        ]
    )
    pipeline_options.view_as(SetupOptions).save_main_session = True

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
        # Read from PubSub and process
        messages = (
            pipeline
            | "Read from PubSub"
            >> ReadFromPubSub(
                subscription=f'projects/{GCP_CONFIG["PROJECT_ID"]}/subscriptions/stock-data-sub'
            )
            | "Parse JSON" >> beam.ParDo(ParseJsonDoFn())
            | "Window"
            >> beam.WindowInto(
                window.FixedWindows(300),  # 5 minutes
                trigger=trigger.Repeatedly(trigger.AfterCount(1)),
                accumulation_mode=trigger.AccumulationMode.DISCARDING,
            )
            | "Group by Key" >> beam.GroupByKey()
            | "Deduplicate" >> beam.ParDo(DedupRecordsFn())
        )

        # Write to separate tables for each stock
        for symbol, config in STOCK_CONFIGS.items():
            _ = (
                messages
                | f"Filter {symbol}" >> beam.Filter(lambda x: x["symbol"] == symbol)
                | f"Write {symbol} to BigQuery"
                >> WriteToBigQuery(
                    table=f'{GCP_CONFIG["PROJECT_ID"]}:{GCP_CONFIG["DATASET_NAME"]}.{config["table_name"]}',
                    schema=table_schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                )
            )


if __name__ == "__main__":
    run_dedup_pipeline()
