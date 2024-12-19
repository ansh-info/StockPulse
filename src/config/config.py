import os

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# GCP Configuration
PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
TOPIC_NAME = os.getenv("TOPIC_NAME")
DATASET_NAME = os.getenv("DATASET_NAME")
TABLE_NAME = os.getenv("TABLE_NAME")

# Alpha Vantage Configuration
API_KEY = os.getenv("API_KEY")
STOCK_SYMBOL = os.getenv("STOCK_SYMBOL")
