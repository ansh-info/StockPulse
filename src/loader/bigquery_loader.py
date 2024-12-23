import json
import time
from datetime import datetime

from google.api_core import retry
from google.cloud import bigquery, pubsub_v1

from config import GCP_CONFIG, STOCK_CONFIGS


class BigQueryLoader:
    def __init__(self):
        self.client = bigquery.Client()
        self.tables = {}
        self.raw_tables = {}
        self.ensure_dataset_and_tables()

    @retry.Retry(predicate=retry.if_exception_type(Exception))
    def ensure_dataset_and_tables(self):
        """Ensure dataset exists and create both raw and processed tables"""
        dataset_id = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}"

        try:
            # Get dataset or create if it doesn't exist
            try:
                dataset = self.client.get_dataset(dataset_id)
            except Exception:
                dataset = bigquery.Dataset(dataset_id)
                dataset.location = "US"
                dataset = self.client.create_dataset(dataset, exists_ok=True)
                print(f"Created dataset {dataset_id}")

            # Schema for processed data
            processed_schema = [
                bigquery.SchemaField("timestamp", "TIMESTAMP"),
                bigquery.SchemaField("symbol", "STRING"),
                bigquery.SchemaField("open", "FLOAT"),
                bigquery.SchemaField("high", "FLOAT"),
                bigquery.SchemaField("low", "FLOAT"),
                bigquery.SchemaField("close", "FLOAT"),
                bigquery.SchemaField("volume", "INTEGER"),
                bigquery.SchemaField("date", "STRING"),
                bigquery.SchemaField("time", "STRING"),
                bigquery.SchemaField("moving_average", "FLOAT"),
                bigquery.SchemaField("cumulative_average", "FLOAT"),
            ]

            # Schema for raw data
            raw_schema = [
                bigquery.SchemaField("timestamp", "TIMESTAMP"),
                bigquery.SchemaField("open", "FLOAT"),
                bigquery.SchemaField("high", "FLOAT"),
                bigquery.SchemaField("low", "FLOAT"),
                bigquery.SchemaField("close", "FLOAT"),
                bigquery.SchemaField("volume", "INTEGER"),
            ]

            for symbol, config in STOCK_CONFIGS.items():
                # Create processed data table
                processed_table_id = f"{dataset_id}.{config['table_name']}"
                try:
                    self.client.get_table(processed_table_id)
                except Exception:
                    table = bigquery.Table(processed_table_id, schema=processed_schema)
                    self.tables[symbol] = self.client.create_table(
                        table, exists_ok=True
                    )
                    print(
                        f"Created processed table for {symbol}: {config['table_name']}"
                    )

                # Create raw data table
                raw_table_id = f"{dataset_id}.{config['table_name']}_raw"
                try:
                    self.client.get_table(raw_table_id)
                except Exception:
                    raw_table = bigquery.Table(raw_table_id, schema=raw_schema)
                    self.raw_tables[symbol] = self.client.create_table(
                        raw_table, exists_ok=True
                    )
                    print(f"Created raw table for {symbol}: {config['table_name']}_raw")

        except Exception as e:
            print(f"Error in ensure_dataset_and_tables: {e}")
            raise

    @retry.Retry(predicate=retry.if_exception_type(Exception))
    def insert_rows(self, table_id: str, rows: list):
        """Insert rows with retry mechanism"""
        return self.client.insert_rows_json(table_id, rows)

    def callback(self, message):
        try:
            data = json.loads(message.data.decode("utf-8"))
            symbol = data["symbol"]

            if symbol not in STOCK_CONFIGS:
                print(f"Unknown symbol received: {symbol}")
                message.nack()
                return

            # Ensure dataset and tables exist before insertion
            self.ensure_dataset_and_tables()

            # Prepare table IDs for both raw and processed data
            processed_table_id = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}.{STOCK_CONFIGS[symbol]['table_name']}"
            raw_table_id = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}.{STOCK_CONFIGS[symbol]['table_name']}_raw"

            # Prepare rows for processed data
            processed_rows = [
                {
                    "timestamp": data["timestamp"],
                    "symbol": data["symbol"],
                    "open": float(data["open"]),
                    "high": float(data["high"]),
                    "low": float(data["low"]),
                    "close": float(data["close"]),
                    "volume": int(data["volume"]),
                    "date": data["date"],
                    "time": data["time"],
                    "moving_average": (
                        float(data["moving_average"])
                        if data["moving_average"] is not None
                        else None
                    ),
                    "cumulative_average": (
                        float(data["cumulative_average"])
                        if data["cumulative_average"] is not None
                        else None
                    ),
                }
            ]

            # Prepare rows for raw data
            raw_rows = [
                {
                    "timestamp": data["timestamp"],
                    "open": float(data["open"]),
                    "high": float(data["high"]),
                    "low": float(data["low"]),
                    "close": float(data["close"]),
                    "volume": int(data["volume"]),
                }
            ]

            max_retries = 3
            retry_count = 0
            while retry_count < max_retries:
                try:
                    # Insert processed data
                    processed_errors = self.insert_rows(
                        processed_table_id, processed_rows
                    )
                    # Insert raw data
                    raw_errors = self.insert_rows(raw_table_id, raw_rows)

                    if processed_errors == [] and raw_errors == []:
                        print(
                            f"Data inserted successfully for {symbol} at {data['timestamp']}"
                        )
                        message.ack()
                        return
                    else:
                        print(f"Processed data errors: {processed_errors}")
                        print(f"Raw data errors: {raw_errors}")
                        retry_count += 1
                        if retry_count < max_retries:
                            time.sleep(2**retry_count)
                        else:
                            message.nack()
                except Exception as e:
                    print(f"Error inserting rows (attempt {retry_count + 1}): {e}")
                    retry_count += 1
                    if retry_count < max_retries:
                        time.sleep(2**retry_count)
                    else:
                        message.nack()
                        raise

        except Exception as e:
            print(f"Error processing message: {e}")
            print(f"Message content: {message.data.decode('utf-8')}")
            message.nack()


def main():
    while True:
        try:
            loader = BigQueryLoader()

            subscriber = pubsub_v1.SubscriberClient()
            subscription_path = subscriber.subscription_path(
                GCP_CONFIG["PROJECT_ID"], "stock-data-sub"
            )

            streaming_pull_future = subscriber.subscribe(
                subscription_path, loader.callback
            )
            print(f"Starting to listen for messages on {subscription_path}")

            streaming_pull_future.result()
        except KeyboardInterrupt:
            print("Stopping the loader...")
            break
        except Exception as e:
            print(f"Error in main loop: {e}")
            print("Restarting loader in 10 seconds...")
            time.sleep(10)


if __name__ == "__main__":
    main()
