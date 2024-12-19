import json
import time
from datetime import datetime

import requests
from google.cloud import pubsub_v1, storage

import config


def initialize_clients():
    """Initialize GCP clients"""
    publisher = pubsub_v1.PublisherClient()
    storage_client = storage.Client()
    return publisher, storage_client


def fetch_stock_data():
    """Fetch stock data from Alpha Vantage"""
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={config.STOCK_SYMBOL}&interval=5min&outputsize=full&apikey={config.API_KEY}"
    response = requests.get(url)
    return response.json()


def publish_to_pubsub(publisher, data):
    """Publish data to Pub/Sub"""
    topic_path = publisher.topic_path(config.PROJECT_ID, config.TOPIC_NAME)
    message = json.dumps(data).encode("utf-8")
    future = publisher.publish(topic_path, data=message)
    return future.result()


def save_to_gcs(storage_client, data):
    """Save raw data to Cloud Storage"""
    bucket = storage_client.bucket(config.BUCKET_NAME)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    blob = bucket.blob(f"raw-data/{config.STOCK_SYMBOL}/{timestamp}.json")
    blob.upload_from_string(json.dumps(data))


def main():
    print("Initializing stock data pipeline...")
    publisher, storage_client = initialize_clients()

    while True:
        try:
            print(f"Fetching data for {config.STOCK_SYMBOL} at {datetime.now()}")
            data = fetch_stock_data()

            if "Time Series (5min)" in data:
                # Save raw data to GCS
                save_to_gcs(storage_client, data)
                print("Raw data saved to Cloud Storage")

                # Process and publish each data point
                for timestamp, values in data["Time Series (5min)"].items():
                    record = {
                        "timestamp": timestamp,
                        "symbol": config.STOCK_SYMBOL,
                        "open": float(values["1. open"]),
                        "high": float(values["2. high"]),
                        "low": float(values["3. low"]),
                        "close": float(values["4. close"]),
                        "volume": int(values["5. volume"]),
                    }
                    message_id = publish_to_pubsub(publisher, record)
                    print(f"Published message {message_id} for timestamp {timestamp}")

                print(f"Data processing completed at {datetime.now()}")
            else:
                print("No time series data found in response")

            # Wait for 5 minutes (Alpha Vantage rate limit)
            print("Waiting for 5 minutes before next fetch...")
            time.sleep(300)

        except Exception as e:
            print(f"Error occurred: {e}")
            print("Retrying in 60 seconds...")
            time.sleep(60)


if __name__ == "__main__":
    main()
