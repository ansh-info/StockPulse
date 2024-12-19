import json
import time

from google.cloud import bigquery, pubsub_v1

PROJECT_ID = "stock-data-pipeline-444011"
SUBSCRIPTION_NAME = "stock-data-sub"
DATASET_NAME = "stock_market"
TABLE_NAME = "amazon_stock"


def create_bigquery_table():
    client = bigquery.Client()
    dataset_ref = f"{PROJECT_ID}.{DATASET_NAME}"
    table_ref = f"{dataset_ref}.{TABLE_NAME}"

    schema = [
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("open", "FLOAT"),
        bigquery.SchemaField("high", "FLOAT"),
        bigquery.SchemaField("low", "FLOAT"),
        bigquery.SchemaField("close", "FLOAT"),
        bigquery.SchemaField("volume", "INTEGER"),
    ]

    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table, exists_ok=True)
    return client


def callback(message):
    try:
        data = json.loads(message.data.decode("utf-8"))
        client = bigquery.Client()
        table_id = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"

        rows_to_insert = [
            {
                "timestamp": data["timestamp"],
                "symbol": data["symbol"],
                "open": data["open"],
                "high": data["high"],
                "low": data["low"],
                "close": data["close"],
                "volume": data["volume"],
            }
        ]

        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors == []:
            print(f"Data inserted successfully for timestamp {data['timestamp']}")
            message.ack()
        else:
            print(f"Errors: {errors}")
            message.nack()

    except Exception as e:
        print(f"Error processing message: {e}")
        message.nack()


def main():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

    # Create BigQuery table if it doesn't exist
    create_bigquery_table()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback)
    print(f"Starting to listen for messages on {subscription_path}")

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("Stopped listening for messages")


if __name__ == "__main__":
    main()
