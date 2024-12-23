import json
import time
from datetime import datetime
from typing import Dict

import requests
import schedule
from data_preprocessor import DataPreprocessor
from google.cloud import pubsub_v1, storage

from config import GCP_CONFIG, STOCK_CONFIGS, get_api_url


class StockDataPipeline:
    def __init__(self):
        self.publisher = pubsub_v1.PublisherClient()
        self.storage_client = storage.Client()
        self.preprocessor = DataPreprocessor()
        self.topic_path = self.publisher.topic_path(
            GCP_CONFIG["PROJECT_ID"], GCP_CONFIG["TOPIC_NAME"]
        )

    def save_to_gcs(self, data: Dict, symbol: str, timestamp: str) -> None:
        """Save raw data to Google Cloud Storage"""
        try:
            bucket = self.storage_client.bucket(GCP_CONFIG["BUCKET_NAME"])
            blob = bucket.blob(f"raw-data/{symbol}/{timestamp}.json")
            blob.upload_from_string(json.dumps(data))
            print(f"Saved raw JSON to GCS: {symbol} - {timestamp}")
        except Exception as e:
            print(f"Error saving to GCS: {e}")

    def publish_to_pubsub(
        self, timestamp: str, symbol: str, values: Dict, processed_data: Dict
    ) -> None:
        """Publish record to Pub/Sub"""
        try:
            # Combine raw and processed data
            record = {
                "timestamp": timestamp,
                "symbol": symbol,
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "volume": int(values["5. volume"]),
                "date": processed_data.get(timestamp, {}).get("date"),
                "time": processed_data.get(timestamp, {}).get("time"),
                "moving_average": processed_data.get(timestamp, {}).get(
                    "moving_average"
                ),
                "cumulative_average": processed_data.get(timestamp, {}).get(
                    "cumulative_average"
                ),
            }

            message = json.dumps(record).encode("utf-8")
            future = self.publisher.publish(self.topic_path, data=message)
            message_id = future.result()
            print(f"Published message {message_id} for {symbol} at {timestamp}")
        except Exception as e:
            print(f"Error publishing to Pub/Sub: {e}")
            print(f"Record content: {record}")

    def fetch_stock_data(self, symbol: str, config: Dict) -> None:
        """Fetch and process data for a single stock"""
        api_url = get_api_url(
            symbol=symbol, interval=config["interval"], api_key=config["api_key"]
        )

        try:
            r = requests.get(api_url)
            data = r.json()

            if "Time Series (5min)" in data:
                time_series = data["Time Series (5min)"]
                current_time = datetime.now().strftime("%Y%m%d_%H%M%S")

                # Save raw data to GCS
                self.save_to_gcs(data, symbol, current_time)

                # Preprocess the time series data
                processed_data = self.preprocessor.preprocess_time_series(time_series)

                # Process and publish each data point
                for timestamp, values in time_series.items():
                    self.publish_to_pubsub(timestamp, symbol, values, processed_data)

                print(f"Successfully processed data for {symbol}")
            else:
                print(f"Error: Time Series data not found in the response for {symbol}")

        except Exception as e:
            print(f"Error processing {symbol}: {e}")
            print(f"Full error details: {str(e)}")

        # Add delay to respect rate limits
        time.sleep(12)  # Alpha Vantage free tier allows 5 calls per minute


def main():
    pipeline = StockDataPipeline()

    def process_all_stocks():
        """Process all configured stocks"""
        for symbol, config in STOCK_CONFIGS.items():
            pipeline.fetch_stock_data(symbol, config)

    # Schedule the task to run every hour
    schedule.every(1).hour.do(process_all_stocks)

    # Run immediately for the first time
    print("Starting initial data fetch...")
    process_all_stocks()

    # Keep the script running
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            print("Stopping the pipeline...")
            break
        except Exception as e:
            print(f"Error occurred: {e}")
            time.sleep(60)


if __name__ == "__main__":
    main()
