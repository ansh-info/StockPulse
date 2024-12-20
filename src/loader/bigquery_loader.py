import json

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

    def process_and_load_data(self, data: dict, symbol: str):
        """Process raw data and load both raw and processed data into BigQuery"""
        try:
            # Create DataFrame from the message data
            raw_df = pd.DataFrame(
                [
                    {
                        "timestamp": pd.to_datetime(data["timestamp"]),
                        "symbol": data["symbol"],
                        "open": float(data["open"]),
                        "high": float(data["high"]),
                        "low": float(data["low"]),
                        "close": float(data["close"]),
                        "volume": int(data["volume"]),
                    }
                ]
            )

            # Process the data
            processed_df = self.preprocessor.process_stock_data(
                raw_df.copy(),
                resample_freq=None,  # Keep original frequency
                fill_gaps=True,
                calculate_indicators=True,
            )

            # Reset index to make timestamp a column
            processed_df = processed_df.reset_index()

            # Load raw data
            raw_table_id = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}.{STOCK_CONFIGS[symbol]['table_name']}_raw"
            job_raw = self.client.load_table_from_dataframe(raw_df, raw_table_id)
            job_raw.result()  # Wait for the job to complete

            # Load processed data
            processed_table_id = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}.{STOCK_CONFIGS[symbol]['table_name']}_processed"
            job_processed = self.client.load_table_from_dataframe(
                processed_df, processed_table_id
            )
            job_processed.result()  # Wait for the job to complete

            print(f"Data loaded successfully for {symbol}")
            return True

        except Exception as e:
            print(f"Error processing and loading data: {e}")
            return False

    def callback(self, message):
        """Handle incoming Pub/Sub messages"""
        try:
            data = json.loads(message.data.decode("utf-8"))
            symbol = data["symbol"]

            if symbol not in STOCK_CONFIGS:
                print(f"Unknown symbol received: {symbol}")
                message.nack()
                return

            # Process and load the data
            if self.process_and_load_data(data, symbol):
                message.ack()
            else:
                message.nack()

        except Exception as e:
            print(f"Error processing message: {e}")
            message.nack()


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
        print("Stopped listening for messages")


if __name__ == "__main__":
    main()
