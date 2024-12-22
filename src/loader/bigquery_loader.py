import json
import logging
import os
import random
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd
from google.cloud import bigquery, pubsub_v1
from preprocessing_pipeline import StockDataPreprocessor

from config import GCP_CONFIG, STOCK_CONFIGS


class BigQueryLoader:
    def __init__(self):
        # Set up logging first
        self.setup_logging()
        self.logger.info("Initializing BigQuery Loader...")

        # Initialize clients
        self.client = bigquery.Client()
        self.preprocessor = StockDataPreprocessor()

        # Initialize table references
        self.raw_tables = {}
        self.processed_tables = {}

        # Initialize batch processing attributes
        self.batch_size = 50  # Batch size for processing
        self.batch_timeout = 240  # Maximum time to hold batch
        self.min_batch_interval = 5.0  # Minimum seconds between batches
        self.batch_data = defaultdict(list)
        self.last_load_time = defaultdict(float)
        self.max_concurrent_loads = 3  # Limit concurrent operations
        self.active_loads = 0

        # Initialize retry parameters
        self.max_retries = 5
        self.initial_retry_delay = 2
        self.max_retry_delay = 64

        # Define column schemas
        self.raw_columns = [
            "timestamp",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]

        self.processed_columns = self.raw_columns + [
            "date",
            "time",
            "ma5",
            "cma",
            "eod_ma5",
        ]

        # Setup infrastructure
        self.logger.info("Setting up BigQuery infrastructure...")
        self.setup_dataset()
        self.setup_tables()
        self.logger.info("Infrastructure setup complete.")

    def setup_logging(self):
        """Configure logging to both file and console"""
        try:
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = log_dir / f"bigquery_loader_{timestamp}.log"

            self.logger = logging.getLogger("BigQueryLoader")
            self.logger.setLevel(logging.INFO)
            self.logger.handlers = []

            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.INFO)
            file_formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s"
            )
            file_handler.setFormatter(file_formatter)

            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s"
            )
            console_handler.setFormatter(console_formatter)

            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

            self.logger.info(f"Logging setup complete. Log file: {log_file}")

        except Exception as e:
            print(f"Error setting up logging: {e}")
            raise

    def wait_for_slot(self):
        """Wait for an available loading slot"""
        while self.active_loads >= self.max_concurrent_loads:
            time.sleep(1)
        self.active_loads += 1
        self.logger.debug(f"Acquired loading slot. Active loads: {self.active_loads}")

    def release_slot(self):
        """Release a loading slot"""
        self.active_loads = max(0, self.active_loads - 1)
        self.logger.debug(f"Released loading slot. Active loads: {self.active_loads}")

    def exponential_backoff(self, attempt):
        """Calculate exponential backoff time with jitter"""
        delay = min(self.max_retry_delay, self.initial_retry_delay * (4**attempt))
        jitter = random.uniform(0, 0.2 * delay)
        return delay + jitter

    def setup_dataset(self):
        """Create the dataset if it doesn't exist"""
        dataset_id = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}"
        try:
            dataset = self.client.get_dataset(dataset_id)
            self.logger.info(f"Dataset {dataset_id} already exists")
        except Exception:
            self.logger.info(f"Creating dataset {dataset_id}...")
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"
            dataset = self.client.create_dataset(dataset, exists_ok=True)
            self.logger.info(f"Created dataset {dataset_id}")

    def setup_tables(self):
        """Create both raw and processed tables with updated schema"""
        self.logger.info("Setting up tables...")
        dataset_ref = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}"

        # Raw table schema (kept exactly as received)
        raw_schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("open", "FLOAT"),
            bigquery.SchemaField("high", "FLOAT"),
            bigquery.SchemaField("low", "FLOAT"),
            bigquery.SchemaField("close", "FLOAT"),
            bigquery.SchemaField("volume", "INTEGER"),
        ]

        # Processed table schema (includes calculated fields)
        processed_schema = raw_schema + [
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("time", "TIME"),
            bigquery.SchemaField("ma5", "FLOAT"),
            bigquery.SchemaField("cma", "FLOAT"),
            bigquery.SchemaField("eod_ma5", "FLOAT"),
        ]

        for symbol, config in STOCK_CONFIGS.items():
            try:
                self.logger.info(f"Setting up tables for {symbol}...")

                # Raw table setup
                raw_table_ref = f"{dataset_ref}.{config['table_name']}_raw"
                raw_table = bigquery.Table(raw_table_ref, schema=raw_schema)
                self.raw_tables[symbol] = self.client.create_table(
                    raw_table, exists_ok=True
                )

                # Processed table setup
                processed_table_ref = f"{dataset_ref}.{config['table_name']}_processed"
                processed_table = bigquery.Table(
                    processed_table_ref, schema=processed_schema
                )
                self.processed_tables[symbol] = self.client.create_table(
                    processed_table, exists_ok=True
                )

                self.logger.info(f"Successfully set up tables for {symbol}")
            except Exception as e:
                self.logger.error(f"Error setting up tables for {symbol}: {e}")

    def load_batch_with_retry(
        self, df: pd.DataFrame, table_id: str, job_config: bigquery.LoadJobConfig
    ) -> bool:
        """Load data to BigQuery with exponential backoff retry"""
        for attempt in range(self.max_retries):
            try:
                job = self.client.load_table_from_dataframe(
                    df, table_id, job_config=job_config
                )
                job.result()
                return True

            except Exception as e:
                error_msg = str(e).lower()

                if "rate limits exceeded" in error_msg:
                    if attempt < self.max_retries - 1:
                        delay = self.exponential_backoff(attempt)
                        self.logger.warning(
                            f"Rate limit exceeded. Retrying in {delay:.2f} seconds. "
                            f"Attempt {attempt + 1}/{self.max_retries}"
                        )
                        time.sleep(delay)
                    else:
                        self.logger.error(
                            f"Failed after {self.max_retries} attempts: {e}"
                        )
                        return False
                else:
                    self.logger.error(f"Error loading batch: {e}")
                    return False
        return False

    def load_batch(self, symbol: str) -> bool:
        """Load a batch of records for a specific symbol"""
        if not self.batch_data[symbol]:
            return True

        try:
            self.wait_for_slot()

            try:
                batch_size = len(self.batch_data[symbol])
                self.logger.info(
                    f"Attempting to load batch of {batch_size} records for {symbol}"
                )

                # Create raw DataFrame and properly handle timestamp
                raw_df = pd.DataFrame(self.batch_data[symbol])
                raw_df["timestamp"] = pd.to_datetime(raw_df["timestamp"])

                # Ensure data types are correct for BigQuery
                raw_df = raw_df.astype(
                    {
                        "symbol": str,
                        "open": float,
                        "high": float,
                        "low": float,
                        "close": float,
                        "volume": int,
                    }
                )

                # Select only raw columns in correct order
                raw_df = raw_df[self.raw_columns]

                # Process data for processed table
                processed_df = self.preprocessor.process_stock_data(raw_df.copy())

                # Ensure processed data types are correct
                processed_df["date"] = pd.to_datetime(processed_df["date"]).dt.date
                processed_df["time"] = pd.to_datetime(
                    processed_df["time"], format="%H:%M:%S"
                ).dt.time
                processed_df = processed_df.astype(
                    {
                        "symbol": str,
                        "open": float,
                        "high": float,
                        "low": float,
                        "close": float,
                        "volume": int,
                        "ma5": float,
                        "cma": float,
                        "eod_ma5": float,
                    }
                )

                processed_df = processed_df[self.processed_columns]

                # Configure job
                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
                )

                # Load raw data
                raw_table_id = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}.{STOCK_CONFIGS[symbol]['table_name']}_raw"
                raw_success = self.load_batch_with_retry(
                    raw_df, raw_table_id, job_config
                )

                if raw_success:
                    # Load processed data
                    processed_table_id = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}.{STOCK_CONFIGS[symbol]['table_name']}_processed"
                    processed_success = self.load_batch_with_retry(
                        processed_df, processed_table_id, job_config
                    )

                    if processed_success:
                        self.logger.info(
                            f"Successfully loaded batch of {batch_size} records for {symbol}"
                        )
                        self.batch_data[symbol] = []
                        self.last_load_time[symbol] = time.time()
                        return True
                    else:
                        self.logger.error("Failed to load processed data batch")
                        return False
                else:
                    self.logger.error("Failed to load raw data batch")
                    return False

            finally:
                self.release_slot()

        except Exception as e:
            self.logger.error(f"Error loading batch for {symbol}: {e}")
            return False

    def should_load_batch(self, symbol: str) -> bool:
        """Check if batch should be loaded"""
        if not self.batch_data[symbol]:
            return False

        current_time = time.time()
        time_since_last_load = current_time - self.last_load_time[symbol]

        if time_since_last_load < self.min_batch_interval:
            return False

        timeout_reached = time_since_last_load > self.batch_timeout
        batch_full = len(self.batch_data[symbol]) >= self.batch_size

        return timeout_reached or batch_full

    def process_and_load_data(self, data: dict, symbol: str) -> bool:
        """Process and potentially load data"""
        try:
            # Add to batch without any modifications
            self.batch_data[symbol].append(data)

            # Check if we should load the batch
            if self.should_load_batch(symbol):
                return self.load_batch(symbol)
            return True

        except Exception as e:
            self.logger.error(f"Error processing data: {e}")
            return False

    def callback(self, message):
        """Handle incoming Pub/Sub messages"""
        try:
            self.logger.debug("Received new message...")
            data = json.loads(message.data.decode("utf-8"))
            symbol = data.get("symbol")

            if not symbol or symbol not in STOCK_CONFIGS:
                self.logger.warning(f"Invalid symbol in message: {symbol}")
                message.ack()
                return

            if self.process_and_load_data(data, symbol):
                message.ack()
                self.logger.debug("Message acknowledged")
            else:
                message.nack()
                self.logger.warning("Message not acknowledged due to processing error")

        except Exception as e:
            self.logger.error(f"Error in callback: {e}")
            message.nack()

    def cleanup(self):
        """Load any remaining batches before shutting down"""
        self.logger.info("Running cleanup...")
        for symbol in list(self.batch_data.keys()):
            if self.batch_data[symbol]:
                self.load_batch(symbol)
        self.logger.info("Cleanup complete")


def main():
    loader = BigQueryLoader()
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        GCP_CONFIG["PROJECT_ID"], "stock-data-sub"
    )

    streaming_pull_future = subscriber.subscribe(subscription_path, loader.callback)
    loader.logger.info(f"Starting to listen for messages on {subscription_path}")

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        loader.cleanup()
        loader.logger.info("Stopped listening for messages")
    except Exception as e:
        loader.logger.error(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
