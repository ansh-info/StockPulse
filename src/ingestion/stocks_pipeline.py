import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Optional

import requests
import schedule
from google.cloud import bigquery, pubsub_v1, storage

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class StockDataPipeline:
    def __init__(self):
        self.publisher = pubsub_v1.PublisherClient()
        self.storage_client = storage.Client()
        self.bigquery_client = bigquery.Client()
        self.preprocessor = DataPreprocessor()
        self.topic_path = self.publisher.topic_path(
            GCP_CONFIG["PROJECT_ID"], GCP_CONFIG["TOPIC_NAME"]
        )
        self.last_fetch_times = {}

    def get_latest_timestamp(self, symbol: str) -> Optional[datetime]:
        """Get the latest timestamp for a symbol from BigQuery"""
        table_id = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}.{STOCK_CONFIGS[symbol]['table_name']}"
        query = f"""
        SELECT MAX(timestamp) as latest_timestamp
        FROM `{table_id}`
        WHERE symbol = '{symbol}'
        """
        try:
            query_job = self.bigquery_client.query(query)
            results = query_job.result()
            for row in results:
                return row.latest_timestamp
        except Exception as e:
            logger.error(f"Error getting latest timestamp for {symbol}: {e}")
            return None

    def should_fetch_data(self, symbol: str) -> bool:
        """Determine if we should fetch new data for a symbol"""
        latest_timestamp = self.get_latest_timestamp(symbol)

        if latest_timestamp is None:
            return True

        current_time = datetime.utcnow()
        time_difference = current_time - latest_timestamp

        # Only fetch if more than 5 minutes have passed since last data point
        return time_difference.total_seconds() > 300  # 5 minutes in seconds

    def fetch_stock_data(self, symbol: str, config: Dict) -> None:
        """Fetch and process data for a single stock"""
        if not self.should_fetch_data(symbol):
            logger.info(f"Skipping {symbol} - No new data expected")
            return

        api_url = get_api_url(
            symbol=symbol, interval=config["interval"], api_key=config["api_key"]
        )

        try:
            logger.info(f"Fetching data for {symbol}")
            r = requests.get(api_url)
            data = r.json()

            if "Time Series (5min)" in data:
                time_series = data["Time Series (5min)"]
                current_time = datetime.now().strftime("%Y%m%d_%H%M%S")

                # Get latest timestamp in BigQuery
                latest_timestamp = self.get_latest_timestamp(symbol)

                # Filter out old data points
                filtered_time_series = {}
                for timestamp, values in time_series.items():
                    dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                    if latest_timestamp is None or dt > latest_timestamp:
                        filtered_time_series[timestamp] = values

                if not filtered_time_series:
                    logger.info(f"No new data points for {symbol}")
                    return

                # Save filtered raw data to GCS
                self.save_to_gcs(
                    {"Time Series (5min)": filtered_time_series}, symbol, current_time
                )

                # Preprocess the filtered time series data
                processed_data = self.preprocessor.preprocess_time_series(
                    filtered_time_series
                )

                # Process and publish each new data point
                for timestamp, values in filtered_time_series.items():
                    self.publish_to_pubsub(timestamp, symbol, values, processed_data)
                    logger.info(f"Published data point for {symbol} at {timestamp}")

                logger.info(
                    f"Successfully processed {len(filtered_time_series)} new data points for {symbol}"
                )
            else:
                logger.error(
                    f"Error: Time Series data not found in the response for {symbol}"
                )

        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            logger.error(f"Full error details: {str(e)}")

        # Add delay to respect rate limits
        time.sleep(12)  # Alpha Vantage free tier allows 5 calls per minute


def main():
    logger.info("Starting Stock Data Pipeline...")
    pipeline = StockDataPipeline()

    def process_all_stocks():
        """Process all configured stocks"""
        logger.info("Starting batch processing of all stocks...")
        for symbol, config in STOCK_CONFIGS.items():
            pipeline.fetch_stock_data(symbol, config)
        logger.info("Completed batch processing of all stocks")

    # Schedule the task to run every hour
    schedule.every(1).hour.do(process_all_stocks)

    # Run immediately for the first time
    logger.info("Starting initial data fetch...")
    process_all_stocks()

    # Keep the script running
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopping the pipeline...")
            break
        except Exception as e:
            logger.error(f"Error occurred: {e}")
            time.sleep(60)


if __name__ == "__main__":
    main()
