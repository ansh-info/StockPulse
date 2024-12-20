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

    def process_and_load_data(self, raw_data_df, symbol):
        """Process raw data and load both raw and processed data into BigQuery"""
        try:
            # Process the data
            processed_df = self.preprocessor.process_stock_data(
                raw_data_df,
                resample_freq=None,  # Keep original 5-min frequency
                fill_gaps=True,
                calculate_indicators=True,
            )

            # Load raw data
            raw_table_id = self.raw_tables[symbol].table_id
            raw_errors = self.client.load_table_from_dataframe(
                raw_data_df, raw_table_id
            ).result()

            # Load processed data
            processed_table_id = self.processed_tables[symbol].table_id
            processed_errors = self.client.load_table_from_dataframe(
                processed_df, processed_table_id
            ).result()

            if not raw_errors and not processed_errors:
                print(f"Data loaded successfully for {symbol}")
                return True
            else:
                print(f"Errors loading data for {symbol}")
                return False

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

            # Convert message to DataFrame for processing
            df = pd.DataFrame([data])

            # Process and load the data
            if self.process_and_load_data(df, symbol):
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
