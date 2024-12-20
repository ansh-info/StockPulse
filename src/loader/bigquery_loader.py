import json
import time
from collections import defaultdict

import pandas as pd
from google.cloud import bigquery, pubsub_v1
from preprocessing_pipeline import StockDataPreprocessor

from config import GCP_CONFIG, STOCK_CONFIGS


class BigQueryLoader:
    def __init__(self):
        self.client = bigquery.Client()
        self.preprocessor = StockDataPreprocessor()
        self.raw_tables = {}
        self.processed_tables = {}
        self.batch_size = 100  # Number of records to batch before loading
        self.batch_timeout = 60  # Seconds to wait before loading a non-full batch
        self.batch_data = defaultdict(list)
        self.last_load_time = defaultdict(float)
        self.setup_tables()

    def setup_tables(self):
        """Create both raw and processed tables for all configured stocks"""
        dataset_ref = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}"

        # Schema for raw data
        raw_schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("open", "FLOAT"),
            bigquery.SchemaField("high", "FLOAT"),
            bigquery.SchemaField("low", "FLOAT"),
            bigquery.SchemaField("close", "FLOAT"),
            bigquery.SchemaField("volume", "INTEGER"),
        ]

        # Extended schema for processed data
        processed_schema = raw_schema + [
            bigquery.SchemaField("daily_return", "FLOAT"),
            bigquery.SchemaField("ma7", "FLOAT"),
            bigquery.SchemaField("ma20", "FLOAT"),
            bigquery.SchemaField("volatility", "FLOAT"),
            bigquery.SchemaField("volume_ma5", "FLOAT"),
            bigquery.SchemaField("momentum", "FLOAT"),
        ]

        for symbol, config in STOCK_CONFIGS.items():
            # Setup raw data table
            raw_table_ref = f"{dataset_ref}.{config['table_name']}_raw"
            raw_table = bigquery.Table(raw_table_ref, schema=raw_schema)
            self.raw_tables[symbol] = self.client.create_table(
                raw_table, exists_ok=True
            )

            # Setup processed data table
            processed_table_ref = f"{dataset_ref}.{config['table_name']}_processed"
            processed_table = bigquery.Table(
                processed_table_ref, schema=processed_schema
            )
            self.processed_tables[symbol] = self.client.create_table(
                processed_table, exists_ok=True
            )

            print(f"Ensured tables exist for {symbol}")

    def load_batch(self, symbol: str):
        """Load a batch of records for a specific symbol"""
        try:
            if not self.batch_data[symbol]:
                return True

            # Convert batch to DataFrame
            raw_df = pd.DataFrame(self.batch_data[symbol])
            raw_df["timestamp"] = pd.to_datetime(raw_df["timestamp"])

            # Process the batch
            df_for_processing = raw_df.copy()
            processed_df = self.preprocessor.process_stock_data(
                df_for_processing,
                resample_freq=None,
                fill_gaps=True,
                calculate_indicators=True,
            )
            processed_df = processed_df.reset_index()

            # Load raw data
            raw_table_id = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}.{STOCK_CONFIGS[symbol]['table_name']}_raw"
            job_raw = self.client.load_table_from_dataframe(raw_df, raw_table_id)
            job_raw.result()

            # Load processed data
            processed_table_id = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}.{STOCK_CONFIGS[symbol]['table_name']}_processed"
            job_processed = self.client.load_table_from_dataframe(
                processed_df, processed_table_id
            )
            job_processed.result()

            print(
                f"Successfully loaded batch of {len(self.batch_data[symbol])} records for {symbol}"
            )
            self.batch_data[symbol] = []
            self.last_load_time[symbol] = time.time()
            return True

        except Exception as e:
            print(f"Error loading batch for {symbol}: {e}")
            return False

    def process_and_load_data(self, data: dict, symbol: str):
        """Add data to batch and load if batch is full or timeout reached"""
        try:
            # Add record to batch
            self.batch_data[symbol].append(
                {
                    "timestamp": data["timestamp"],
                    "symbol": symbol,
                    "open": float(data["open"]),
                    "high": float(data["high"]),
                    "low": float(data["low"]),
                    "close": float(data["close"]),
                    "volume": int(data["volume"]),
                }
            )

            # Check if we should load the batch
            current_time = time.time()
            timeout_reached = (
                current_time - self.last_load_time[symbol]
            ) > self.batch_timeout
            batch_full = len(self.batch_data[symbol]) >= self.batch_size

            if batch_full or timeout_reached:
                return self.load_batch(symbol)
            return True

        except Exception as e:
            print(f"Error processing data: {e}")
            return False

    def callback(self, message):
        """Handle incoming Pub/Sub messages"""
        try:
            print("\nReceived new message...")
            data = json.loads(message.data.decode("utf-8"))
            print(f"Decoded message data: {data}")

            symbol = data.get("symbol")
            if not symbol or symbol not in STOCK_CONFIGS:
                print(f"Invalid symbol in message: {symbol}")
                message.ack()  # Ack invalid messages to remove them from the queue
                return

            if self.process_and_load_data(data, symbol):
                message.ack()
                print("Message acknowledged")
            else:
                message.nack()
                print("Message not acknowledged due to processing error")

        except Exception as e:
            print(f"Error in callback: {e}")
            message.nack()

    def cleanup(self):
        """Load any remaining batches before shutting down"""
        for symbol in self.batch_data.keys():
            if self.batch_data[symbol]:
                self.load_batch(symbol)


def main():
    loader = BigQueryLoader()
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        GCP_CONFIG["PROJECT_ID"], "stock-data-sub"
    )

    streaming_pull_future = subscriber.subscribe(subscription_path, loader.callback)
    print(f"Starting to listen for messages on {subscription_path}")

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        loader.cleanup()  # Load any remaining batches
        print("Stopped listening for messages")


if __name__ == "__main__":
    main()
