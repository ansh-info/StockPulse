import json
import time
from datetime import datetime

import pandas as pd
from google.cloud import pubsub_v1, storage
from preprocessing_pipeline import StockDataPreprocessor

from config import GCP_CONFIG, STOCK_CONFIGS, get_api_url


class StockDataPipeline:
    def __init__(self):
        self.publisher = pubsub_v1.PublisherClient()
        self.storage_client = storage.Client()
        self.preprocessor = StockDataPreprocessor()
        self.topic_path = self.publisher.topic_path(
            GCP_CONFIG["PROJECT_ID"], GCP_CONFIG["TOPIC_NAME"]
        )

    def save_to_gcs(self, data, symbol: str, timestamp: str) -> None:
        """Save both raw and processed data to Google Cloud Storage"""
        try:
            bucket = self.storage_client.bucket(GCP_CONFIG["BUCKET_NAME"])

            # Save raw data
            raw_blob = bucket.blob(f"raw-data/{symbol}/{timestamp}.json")
            raw_blob.upload_from_string(json.dumps(data))

            # Save processed data
            if isinstance(data, dict) and "Time Series (5min)" in data:
                # Convert to DataFrame and process
                df = pd.DataFrame.from_dict(data["Time Series (5min)"], orient="index")
                df.index.name = "timestamp"
                df.columns = [col.split(". ")[1] for col in df.columns]

                processed_df = self.preprocessor.process_stock_data(df)
                processed_blob = bucket.blob(
                    f"processed-data/{symbol}/{timestamp}.json"
                )
                processed_blob.upload_from_string(processed_df.to_json())

            print(f"Saved data to GCS: {symbol} - {timestamp}")

        except Exception as e:
            print(f"Error saving to GCS: {e}")

    def publish_to_pubsub(self, record: dict) -> None:
        """Publish record to Pub/Sub"""
        try:
            # Create DataFrame for preprocessing
            df = pd.DataFrame([record])

            # Process the data
            processed_df = self.preprocessor.process_stock_data(
                df, resample_freq=None, fill_gaps=True, calculate_indicators=True
            )

            # Convert processed data back to record format
            processed_record = processed_df.iloc[0].to_dict()

            # Publish processed data
            message = json.dumps(processed_record).encode("utf-8")
            future = self.publisher.publish(self.topic_path, data=message)
            message_id = future.result()
            print(f"Published message {message_id} for {record['symbol']}")

        except Exception as e:
            print(f"Error publishing to Pub/Sub: {e}")

    def fetch_stock_data(self, symbol: str, config: dict) -> None:
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

                # Save both raw and processed data to GCS
                self.save_to_gcs(data, symbol, current_time)

                # Process and publish each data point
                for timestamp, values in time_series.items():
                    record = {
                        "timestamp": timestamp,
                        "symbol": symbol,
                        "open": float(values["1. open"]),
                        "high": float(values["2. high"]),
                        "low": float(values["3. low"]),
                        "close": float(values["4. close"]),
                        "volume": int(values["5. volume"]),
                    }
                    self.publish_to_pubsub(record)

                print(f"Successfully processed data for {symbol}")
            else:
                print(f"Error: Time Series data not found in the response for {symbol}")

        except Exception as e:
            print(f"Error processing {symbol}: {e}")

        time.sleep(12)  # Rate limiting


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
