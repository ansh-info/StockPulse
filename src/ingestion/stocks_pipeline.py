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
            # Save raw JSON
            bucket = self.storage_client.bucket(GCP_CONFIG["BUCKET_NAME"])
            blob = bucket.blob(f"raw-data/{symbol}/{timestamp}.json")
            blob.upload_from_string(json.dumps(data))
            print(f"Saved raw JSON to GCS: {symbol} - {timestamp}")

            # Save raw CSV and processed data
            self.preprocessor.save_raw_csv(data, symbol, timestamp)
            processed_data = self.preprocessor.process_and_save_data(
                data, symbol, timestamp
            )
            return processed_data

        except Exception as e:
            print(f"Error saving to GCS: {e}")
            return None

    def publish_to_pubsub(self, raw_record: Dict, processed_data: Dict) -> None:
        """Publish record to Pub/Sub with processed data"""
        try:
            # Combine raw and processed data
            record = {
                "timestamp": raw_record["timestamp"],
                "symbol": raw_record["symbol"],
                "open": float(raw_record["open"]),
                "high": float(raw_record["high"]),
                "low": float(raw_record["low"]),
                "close": float(raw_record["close"]),
                "volume": int(raw_record["volume"]),
                "date": processed_data.get("date"),
                "time": processed_data.get("time"),
                "moving_average": processed_data.get("moving_average"),
                "cumulative_average": processed_data.get("cumulative_average"),
            }

            message = json.dumps(record).encode("utf-8")
            future = self.publisher.publish(self.topic_path, data=message)
            message_id = future.result()
            print(f"Published message {message_id} for {record['symbol']}")
        except Exception as e:
            print(f"Error publishing to Pub/Sub: {e}")

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

                # Save raw data and get processed data
                processed_data = self.save_to_gcs(data, symbol, current_time)

                if processed_data:
                    # Process and publish each data point
                    for timestamp, values in time_series.items():
                        raw_record = {
                            "timestamp": timestamp,
                            "symbol": symbol,
                            "open": values["1. open"],
                            "high": values["2. high"],
                            "low": values["3. low"],
                            "close": values["4. close"],
                            "volume": values["5. volume"],
                        }

                        # Get processed data for this timestamp
                        timestamp_processed_data = {
                            "date": processed_data["date"].get(timestamp),
                            "time": processed_data["time"].get(timestamp),
                            "moving_average": processed_data["moving_average"].get(
                                timestamp
                            ),
                            "cumulative_average": processed_data[
                                "cumulative_average"
                            ].get(timestamp),
                        }

                        self.publish_to_pubsub(raw_record, timestamp_processed_data)

                print(f"Successfully processed data for {symbol}")
            else:
                print(f"Error: Time Series data not found in the response for {symbol}")

        except Exception as e:
            print(f"Error processing {symbol}: {e}")

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
