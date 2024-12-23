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
        self.ensure_dataset_and_tables()

    @retry.Retry(predicate=retry.if_exception_type(Exception))
    def ensure_dataset_and_tables(self):
        """Ensure dataset exists and create tables with retry mechanism"""
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

            # Create tables if they don't exist
            schema = [
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

            for symbol, config in STOCK_CONFIGS.items():
                table_id = f"{dataset_id}.{config['table_name']}"
                try:
                    self.client.get_table(table_id)
                except Exception:
                    table = bigquery.Table(table_id, schema=schema)
                    self.tables[symbol] = self.client.create_table(
                        table, exists_ok=True
                    )
                    print(f"Created table for {symbol}: {config['table_name']}")

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

            table_id = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}.{STOCK_CONFIGS[symbol]['table_name']}"

            rows_to_insert = [
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

            max_retries = 3
            retry_count = 0
            while retry_count < max_retries:
                try:
                    errors = self.insert_rows(table_id, rows_to_insert)
                    if errors == []:
                        print(
                            f"Data inserted successfully for {symbol} at {data['timestamp']}"
                        )
                        message.ack()
                        return
                    else:
                        print(f"Errors: {errors}")
                        retry_count += 1
                        if retry_count < max_retries:
                            time.sleep(2**retry_count)  # Exponential backoff
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
