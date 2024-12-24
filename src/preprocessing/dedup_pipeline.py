import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple

import apache_beam as beam
import pandas as pd
from apache_beam import window
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.transforms import trigger
from data_preprocessor import DataPreprocessor

# Import your existing configurations and preprocessor
from config import GCP_CONFIG, STOCK_CONFIGS

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ParseJsonDoFn(beam.DoFn):
    """Parse JSON messages from PubSub."""

    def process(self, element):
        try:
            record = json.loads(element.decode("utf-8"))
            unique_key = f"{record['symbol']}_{record['timestamp']}"
            yield (unique_key, record)
        except Exception as e:
            logger.error(f"Error parsing message: {e}")


class PreprocessDataFn(beam.DoFn):
    """Apply data preprocessing logic."""

    def setup(self):
        self.preprocessor = DataPreprocessor()

    def process(self, element: Dict):
        try:
            # Extract data for preprocessing
            time_series = {
                element["timestamp"]: {
                    "1. open": str(element["open"]),
                    "2. high": str(element["high"]),
                    "3. low": str(element["low"]),
                    "4. close": str(element["close"]),
                    "5. volume": str(element["volume"]),
                }
            }

            # Preprocess using your existing logic
            processed_data = self.preprocessor.preprocess_time_series(time_series)

            if processed_data and element["timestamp"] in processed_data:
                # Update element with processed data
                element.update(
                    {
                        "moving_average": processed_data[element["timestamp"]][
                            "moving_average"
                        ],
                        "cumulative_average": processed_data[element["timestamp"]][
                            "cumulative_average"
                        ],
                        "date": processed_data[element["timestamp"]]["date"],
                        "time": processed_data[element["timestamp"]]["time"],
                    }
                )

            yield element
        except Exception as e:
            logger.error(f"Error in preprocessing: {e}")


class DedupRecordsFn(beam.DoFn):
    """Deduplicate records within each window."""

    def process(self, element: Tuple[str, List[Dict]]):
        key, records = element
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
            f'--project={GCP_CONFIG["PROJECT_ID"]}',
            "--region=us-central1",
            "--streaming",
            "--runner=DataflowRunner",
        ]
    )
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Create separate schemas for each stock
    stock_schemas = {}
    for symbol, config in STOCK_CONFIGS.items():
        stock_schemas[symbol] = {
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
            | "Preprocess Data" >> beam.ParDo(PreprocessDataFn())
        )

        # Write to separate tables for each stock
        for symbol, config in STOCK_CONFIGS.items():
            stock_data = (
                messages
                | f"Filter {symbol}" >> beam.Filter(lambda x: x["symbol"] == symbol)
                | f"Write {symbol} to BigQuery"
                >> WriteToBigQuery(
                    table=f'{GCP_CONFIG["DATASET_NAME"]}.{config["table_name"]}',
                    schema=stock_schemas[symbol],
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                )
            )


if __name__ == "__main__":
    run_dedup_pipeline()
