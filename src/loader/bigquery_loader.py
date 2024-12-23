import json
import time

from google.cloud import bigquery, pubsub_v1

from config import GCP_CONFIG, STOCK_CONFIGS


class BigQueryLoader:
    def __init__(self):
        self.client = bigquery.Client()
        self.tables = {}
        self.setup_dataset()
        self.setup_tables()

    def setup_dataset(self):
        """Create dataset if it doesn't exist"""
        dataset_id = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"  # Specify the location

        try:
            dataset = self.client.create_dataset(dataset, exists_ok=True)
            print(f"Dataset {dataset_id} created or already exists.")
        except Exception as e:
            print(f"Error creating dataset: {e}")
            raise

    def delete_extra_tables(self):
        """Delete the _raw and _processed tables if they exist"""
        dataset_ref = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}"

        for symbol, config in STOCK_CONFIGS.items():
            base_name = config["table_name"]
            extra_tables = [f"{base_name}_raw", f"{base_name}_processed"]

            for table_name in extra_tables:
                table_ref = f"{dataset_ref}.{table_name}"
                try:
                    self.client.delete_table(table_ref)
                    print(f"Deleted extra table: {table_name}")
                except Exception as e:
                    print(
                        f"Note: Table {table_name} does not exist or could not be deleted: {e}"
                    )

    def setup_tables(self):
        """Create tables for all configured stocks if they don't exist"""
        dataset_ref = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}"

        # Single schema for both raw and processed data
        schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("open", "FLOAT"),
            bigquery.SchemaField("high", "FLOAT"),
            bigquery.SchemaField("low", "FLOAT"),
            bigquery.SchemaField("close", "FLOAT"),
            bigquery.SchemaField("volume", "INTEGER"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("time", "TIME"),
            bigquery.SchemaField("moving_average", "FLOAT"),
            bigquery.SchemaField("cumulative_average", "FLOAT"),
        ]

        for symbol, config in STOCK_CONFIGS.items():
            table_id = f"{dataset_ref}.{config['table_name']}"
            table = bigquery.Table(table_id, schema=schema)

            try:
                self.tables[symbol] = self.client.create_table(table, exists_ok=True)
                print(f"Ensured table exists for {symbol}: {config['table_name']}")
            except Exception as e:
                print(f"Error creating table for {symbol}: {e}")
                raise

    def callback(self, message):
        try:
            data = json.loads(message.data.decode("utf-8"))
            symbol = data["symbol"]

            if symbol not in STOCK_CONFIGS:
                print(f"Unknown symbol received: {symbol}")
                message.nack()
                return

            table_id = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}.{STOCK_CONFIGS[symbol]['table_name']}"

            # Convert timestamp string to datetime
            timestamp = data["timestamp"]

            # Convert date and time strings to appropriate formats
            date_str = data.get("date")
            time_str = data.get("time")

            rows_to_insert = [
                {
                    "timestamp": timestamp,
                    "symbol": data["symbol"],
                    "open": float(data["open"]),
                    "high": float(data["high"]),
                    "low": float(data["low"]),
                    "close": float(data["close"]),
                    "volume": int(data["volume"]),
                    "date": date_str,
                    "time": time_str,
                    "moving_average": (
                        float(data.get("moving_average", 0))
                        if data.get("moving_average") is not None
                        else None
                    ),
                    "cumulative_average": (
                        float(data.get("cumulative_average", 0))
                        if data.get("cumulative_average") is not None
                        else None
                    ),
                }
            ]

            errors = self.client.insert_rows_json(table_id, rows_to_insert)
            if errors == []:
                print(f"Data inserted successfully for {symbol} at {timestamp}")
                message.ack()
            else:
                print(f"Errors: {errors}")
                message.nack()

        except Exception as e:
            print(f"Error processing message: {e}")
            print(f"Message content: {message.data.decode('utf-8')}")
            message.nack()


def main():
    try:
        loader = BigQueryLoader()

        # First, clean up extra tables
        loader.delete_extra_tables()

        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(
            GCP_CONFIG["PROJECT_ID"], "stock-data-sub"
        )

        streaming_pull_future = subscriber.subscribe(subscription_path, loader.callback)
        print(f"Starting to listen for messages on {subscription_path}")

        streaming_pull_future.result()
    except KeyboardInterrupt:
        if "streaming_pull_future" in locals():
            streaming_pull_future.cancel()
        print("Stopped listening for messages")
    except Exception as e:
        print(f"Error in main: {e}")
        raise


if __name__ == "__main__":
    main()
