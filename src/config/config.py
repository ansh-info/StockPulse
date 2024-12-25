import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.append(project_root)
    print(f"Added to Python path: {project_root}")


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
    "TSLA": {
        "api_key": "J30SRXLUMQK4EW8Y",
        "table_name": "tesla_stock",
        "interval": "5min",
    },
    "PFE": {
        "api_key": "2RW2VSDAMEKSTSSV",
        "table_name": "pfizer_stock",
        "interval": "5min",
    },
    "JPM": {
        "api_key": "2RW2VSDAMEKSTSSV",
        "table_name": "jpmorgan_stock",
        "interval": "5min",
    },
    "IBM": {
        "api_key": "4R98MITURZRRFCAW",
        "table_name": "ibm_stock",
        "interval": "5min",
    },
    "XOM": {
        "api_key": "2RW2VSDAMEKSTSSV",
        "table_name": "exxonmobil_stock",
        "interval": "5min",
    },
    "KO": {
        "api_key": "2RW2VSDAMEKSTSSV",
        "table_name": "cocacola_stock",
        "interval": "5min",
    },
    "AAPL": {
        "api_key": "4R98MITURZRRFCAW",
        "table_name": "apple_stock",
        "interval": "5min",
    },
    "MSFT": {
        "api_key": "DYDIKRBOWNRQ9CUA",
        "table_name": "microsoft_stock",
        "interval": "5min",
    },
    "GOOGL": {
        "api_key": "DYDIKRBOWNRQ9CUA",
        "table_name": "google_stock",
        "interval": "5min",
    },
    "NVDA": {
        "api_key": "DYDIKRBOWNRQ9CUA",
        "table_name": "nvidia_stock",
        "interval": "5min",
    },
}


def get_api_url(symbol: str, interval: str, api_key: str) -> str:
    return f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&outputsize=full&apikey={api_key}"
