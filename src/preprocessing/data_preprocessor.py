import json
from datetime import datetime
from typing import Dict

import pandas as pd
from google.cloud import storage

from config import GCP_CONFIG


class DataPreprocessor:
    def __init__(self):
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(GCP_CONFIG["BUCKET_NAME"])

    def save_raw_csv(self, data: Dict, symbol: str, timestamp: str) -> None:
        """Save raw data as CSV to Google Cloud Storage"""
        try:
            # Extract time series data
            time_series = data["Time Series (5min)"]

            # Convert to DataFrame
            df = pd.DataFrame.from_dict(time_series, orient="index")
            df.reset_index(inplace=True)

            # Rename columns
            df.columns = ["timestamp", "open", "high", "low", "close", "volume"]

            # Clean numeric columns
            for col in ["open", "high", "low", "close"]:
                df[col] = pd.to_numeric(df[col].str.strip("1234. "))
            df["volume"] = pd.to_numeric(df["volume"].str.strip("5. "))

            # Save as CSV
            blob = self.bucket.blob(f"raw-data/{symbol}/{timestamp}.csv")
            blob.upload_from_string(df.to_csv(index=False))
            print(f"Saved raw CSV to GCS: {symbol} - {timestamp}")

        except Exception as e:
            print(f"Error saving raw CSV to GCS: {e}")

    def process_and_save_data(self, data: Dict, symbol: str, timestamp: str) -> None:
        """Process raw data and save processed version"""
        try:
            # Extract time series data
            time_series = data["Time Series (5min)"]

            # Convert to DataFrame
            df = pd.DataFrame.from_dict(time_series, orient="index")
            df.reset_index(inplace=True)

            # Rename columns
            df.columns = ["timestamp", "open", "high", "low", "close", "volume"]

            # Clean numeric columns
            for col in ["open", "high", "low", "close"]:
                df[col] = pd.to_numeric(df[col].str.strip("1234. "))
            df["volume"] = pd.to_numeric(df["volume"].str.strip("5. "))

            # Convert timestamp to datetime
            df["timestamp"] = pd.to_datetime(df["timestamp"])

            # Add date and time columns
            df["date"] = df["timestamp"].dt.date
            df["time"] = df["timestamp"].dt.time

            # Sort chronologically
            df = df.sort_values(by=["date", "time"]).reset_index(drop=True)

            # Calculate moving averages
            df["moving_average"] = df.groupby("date")["close"].transform(
                lambda x: x.rolling(window=5, min_periods=1).mean()
            )
            df["cumulative_average"] = df["close"].expanding().mean()

            # Save processed CSV
            blob = self.bucket.blob(
                f"processed-data/{symbol}/{timestamp}_processed.csv"
            )
            blob.upload_from_string(df.to_csv(index=False))
            print(f"Saved processed data to GCS: {symbol} - {timestamp}")

        except Exception as e:
            print(f"Error processing and saving data: {e}")
