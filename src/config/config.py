# config.py
from typing import Dict, List

# GCP Configuration
GCP_CONFIG = {
    "PROJECT_ID": "stock-data-pipeline-444011",
    "BUCKET_NAME": "stock-data-pipeline-bucket",
    "TOPIC_NAME": "stock-data",
    "DATASET_NAME": "stock_market",
}

# Stock Configuration
STOCK_CONFIGS = {
    "AMZN": {
        "api_key": "J30SRXLUMQK4EW8Y",
        "table_name": "amazon_stock",
        "interval": "5min",
    },
    "GOOGL": {
        "api_key": "J30SRXLUMQK4EW8Y",  # Replace with actual API key
        "table_name": "google_stock",
        "interval": "5min",
    },
    "MSFT": {
        "api_key": "J30SRXLUMQK4EW8Y",  # Replace with actual API key
        "table_name": "microsoft_stock",
        "interval": "5min",
    },
    # Add more stocks as needed
}


def get_api_url(symbol: str, interval: str, api_key: str) -> str:
    return f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&outputsize=full&apikey={api_key}"
