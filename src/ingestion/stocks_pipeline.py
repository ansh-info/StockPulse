import csv
import json
import os
import time
from datetime import datetime

import requests
import schedule
from google.cloud import pubsub_v1, storage

# GCP Configuration
PROJECT_ID = "stock-data-pipeline-444011"
BUCKET_NAME = "stock-data-pipeline-bucket"
TOPIC_NAME = "stock-data"

# Constants
STOCK_FILENAME = "amazon_stock_data.csv"
LOG_FILENAME = "update_log.txt"
API_URL = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=AMZN&interval=5min&outputsize=full&apikey=J30SRXLUMQK4EW8Y"

# Initialize GCP clients
publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)


def load_existing_timestamps():
    if not os.path.exists(STOCK_FILENAME):
        return set()
    with open(STOCK_FILENAME, mode="r") as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        return {row[0] for row in reader}


def save_to_gcs(data, timestamp):
    """Save data to Google Cloud Storage"""
    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"raw-data/AMZN/{timestamp}.json")
        blob.upload_from_string(json.dumps(data))
        print(f"Saved raw data to GCS: {timestamp}")
    except Exception as e:
        print(f"Error saving to GCS: {e}")


def publish_to_pubsub(record):
    """Publish record to Pub/Sub"""
    try:
        message = json.dumps(record).encode("utf-8")
        future = publisher.publish(topic_path, data=message)
        message_id = future.result()
        print(f"Published message {message_id}")
    except Exception as e:
        print(f"Error publishing to Pub/Sub: {e}")


def fetch_stock_data():
    # Fetch data from the API
    r = requests.get(API_URL)
    data = r.json()

    if "Time Series (5min)" in data:
        time_series = data["Time Series (5min)"]
        existing_timestamps = load_existing_timestamps()
        new_rows = []

        # Save raw response to GCS
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_to_gcs(data, current_time)

        for timestamp, values in time_series.items():
            if timestamp not in existing_timestamps:
                # Prepare row for CSV
                row = [
                    timestamp,
                    values["1. open"],
                    values["2. high"],
                    values["3. low"],
                    values["4. close"],
                    values["5. volume"],
                ]
                new_rows.append(row)

                # Prepare and publish record to Pub/Sub
                record = {
                    "timestamp": timestamp,
                    "symbol": "AMZN",
                    "open": float(values["1. open"]),
                    "high": float(values["2. high"]),
                    "low": float(values["3. low"]),
                    "close": float(values["4. close"]),
                    "volume": int(values["5. volume"]),
                }
                publish_to_pubsub(record)

        if new_rows:
            # Save to local CSV
            file_exists = os.path.exists(STOCK_FILENAME)
            with open(STOCK_FILENAME, mode="a", newline="") as file:
                writer = csv.writer(file)
                if not file_exists:
                    header = ["Timestamp", "Open", "High", "Low", "Close", "Volume"]
                    writer.writerow(header)
                writer.writerows(new_rows)

            # Log the update
            with open(LOG_FILENAME, mode="a") as log_file:
                log_file.write(
                    f"{datetime.now()}: Added {len(new_rows)} new rows to {STOCK_FILENAME}\n"
                )
            print(f"Data successfully updated. Added {len(new_rows)} new rows.")

            # Upload CSV to GCS
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"processed-data/AMZN/{STOCK_FILENAME}")
            blob.upload_from_filename(STOCK_FILENAME)
            print(f"Uploaded {STOCK_FILENAME} to GCS")
        else:
            print("No new data to update.")
    else:
        print("Error: Time Series data not found in the response")


def main():
    print("Starting stock data pipeline...")
    # Schedule the task to run every hour
    schedule.every(1).hour.do(fetch_stock_data)

    # Run the function immediately for the first iteration
    print("Fetching data for the first time...")
    fetch_stock_data()

    print("Scheduler is running. Press Ctrl+C to stop.")
    # Keep the script running to execute the scheduler
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            print("Stopping the pipeline...")
            break
        except Exception as e:
            print(f"Error occurred: {e}")
            time.sleep(60)  # Wait before retrying


if __name__ == "__main__":
    main()
