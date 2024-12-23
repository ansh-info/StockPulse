import json
import time

from google.cloud import bigquery, pubsub_v1

from config import GCP_CONFIG, STOCK_CONFIGS


class BigQueryLoader:
    def __init__(self):
        self.client = bigquery.Client()
        self.tables = {}
        self.setup_tables()

    def setup_tables(self):
        """Create tables for all configured stocks if they don't exist"""
        dataset_ref = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}"

        # Updated schema with preprocessing fields
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
            table_ref = f"{dataset_ref}.{config['table_name']}"
            table = bigquery.Table(table_ref, schema=schema)
            self.tables[symbol] = self.client.create_table(table, exists_ok=True)
            print(f"Ensured table exists for {symbol}: {config['table_name']}")

    def callback(self, message):
        try:
            data = json.loads(message.data.decode("utf-8"))
            symbol = data["symbol"]

            if symbol not in STOCK_CONFIGS:
                print(f"Unknown symbol received: {symbol}")
                message.nack()
                return

            table_id = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}.{STOCK_CONFIGS[symbol]['table_name']}"

            # Updated row structure to include preprocessing fields
            rows_to_insert = [
                {
                    "timestamp": data["timestamp"],
                    "symbol": data["symbol"],
                    "open": data["open"],
                    "high": data["high"],
                    "low": data["low"],
                    "close": data["close"],
                    "volume": data["volume"],
                    "date": data.get("date"),  # New field
                    "time": data.get("time"),  # New field
                    "moving_average": data.get("moving_average"),  # New field
                    "cumulative_average": data.get("cumulative_average"),  # New field
                }
            ]

            errors = self.client.insert_rows_json(table_id, rows_to_insert)
            if errors == []:
                print(f"Data inserted successfully for {symbol} at {data['timestamp']}")
                message.ack()
            else:
                print(f"Errors: {errors}")
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
