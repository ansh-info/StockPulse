import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.append(project_root)
    print(f"Added to Python path: {project_root}")

# GCP Configuration
GCP_CONFIG = {
    "PROJECT_ID": os.getenv("GCP_PROJECT_ID"),
    "BUCKET_NAME": os.getenv("GCP_BUCKET_NAME"),
    "TOPIC_NAME": os.getenv("GCP_TOPIC_NAME"),
    "DATASET_NAME": os.getenv("GCP_DATASET_NAME"),
}

# Stock Configuration
STOCK_CONFIGS = {
    "AMZN": {
        "api_key": os.getenv("ALPHA_VANTAGE_KEY_1"),
        "table_name": "amazon_stock",
        "interval": "5min",
    },
    "TSLA": {
        "api_key": os.getenv("ALPHA_VANTAGE_KEY_1"),
        "table_name": "tesla_stock",
        "interval": "5min",
    },
    "PFE": {
        "api_key": os.getenv("ALPHA_VANTAGE_KEY_2"),
        "table_name": "pfizer_stock",
        "interval": "5min",
    },
    "JPM": {
        "api_key": os.getenv("ALPHA_VANTAGE_KEY_2"),
        "table_name": "jpmorgan_stock",
        "interval": "5min",
    },
    "IBM": {
        "api_key": os.getenv("ALPHA_VANTAGE_KEY_3"),
        "table_name": "ibm_stock",
        "interval": "5min",
    },
    "XOM": {
        "api_key": os.getenv("ALPHA_VANTAGE_KEY_2"),
        "table_name": "exxonmobil_stock",
        "interval": "5min",
    },
    "KO": {
        "api_key": os.getenv("ALPHA_VANTAGE_KEY_2"),
        "table_name": "cocacola_stock",
        "interval": "5min",
    },
    "AAPL": {
        "api_key": os.getenv("ALPHA_VANTAGE_KEY_3"),
        "table_name": "apple_stock",
        "interval": "5min",
    },
    "MSFT": {
        "api_key": os.getenv("ALPHA_VANTAGE_KEY_4"),
        "table_name": "microsoft_stock",
        "interval": "5min",
    },
    "GOOGL": {
        "api_key": os.getenv("ALPHA_VANTAGE_KEY_4"),
        "table_name": "google_stock",
        "interval": "5min",
    },
    "NVDA": {
        "api_key": os.getenv("ALPHA_VANTAGE_KEY_4"),
        "table_name": "nvidia_stock",
        "interval": "5min",
    },
}


def get_api_url(symbol: str, interval: str, api_key: str) -> str:
    return f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&outputsize=full&apikey={api_key}"
