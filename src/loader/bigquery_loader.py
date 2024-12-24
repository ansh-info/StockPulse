import json
import logging
import time
from datetime import datetime
from typing import Dict, List

from google.api_core import retry
from google.cloud import bigquery, pubsub_v1

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class BigQueryLoader:
    def __init__(self):
        self.client = bigquery.Client()
        self.tables = {}
        self.raw_tables = {}
        self.message_buffer = {}  # Buffer for batch processing
        self.buffer_size = 100  # Number of messages to buffer before insertion
        self.buffer_timeout = 60  # Maximum seconds to hold messages in buffer
        self.last_flush_time = time.time()
        self.ensure_dataset_and_tables()

    def check_duplicate(self, table_id: str, timestamp: str, symbol: str) -> bool:
        """Check if a record already exists in BigQuery"""
        query = f"""
        SELECT COUNT(*) as count
        FROM `{table_id}`
        WHERE timestamp = '{timestamp}'
        AND symbol = '{symbol}'
        """
        try:
            query_job = self.client.query(query)
            results = query_job.result()
            for row in results:
                return row.count > 0
        except Exception as e:
            logger.error(f"Error checking duplicates: {e}")
            return False

    def buffer_message(self, table_id: str, row: Dict, is_raw: bool = False):
        """Buffer messages for batch processing"""
        if table_id not in self.message_buffer:
            self.message_buffer[table_id] = []

        self.message_buffer[table_id].append(row)

        # Check if we should flush the buffer
        current_time = time.time()
        should_flush = (
            len(self.message_buffer[table_id]) >= self.buffer_size
            or current_time - self.last_flush_time >= self.buffer_timeout
        )

        if should_flush:
            self.flush_buffer(table_id)

    def flush_buffer(self, table_id: str):
        """Flush buffered messages to BigQuery"""
        if table_id not in self.message_buffer or not self.message_buffer[table_id]:
            return

        rows = self.message_buffer[table_id]
        logger.info(f"Flushing {len(rows)} rows to {table_id}")

        try:
            errors = self.insert_rows(table_id, rows)
            if not errors:
                logger.info(f"Successfully inserted {len(rows)} rows to {table_id}")
                self.message_buffer[table_id] = []
                self.last_flush_time = time.time()
            else:
                logger.error(f"Errors during batch insertion: {errors}")
        except Exception as e:
            logger.error(f"Error flushing buffer: {e}")

    def callback(self, message):
        try:
            data = json.loads(message.data.decode("utf-8"))
            symbol = data["symbol"]
            timestamp = data["timestamp"]

            if symbol not in STOCK_CONFIGS:
                logger.warning(f"Unknown symbol received: {symbol}")
                message.nack()
                return

            processed_table_id = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}.{STOCK_CONFIGS[symbol]['table_name']}"
            raw_table_id = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}.{STOCK_CONFIGS[symbol]['table_name']}_raw"

            # Check for duplicates
            if self.check_duplicate(processed_table_id, timestamp, symbol):
                logger.info(
                    f"Duplicate record found for {symbol} at {timestamp}, skipping..."
                )
                message.ack()
                return

            # Prepare processed data row
            processed_row = {
                "timestamp": timestamp,
                "symbol": symbol,
                "open": float(data["open"]),
                "high": float(data["high"]),
                "low": float(data["low"]),
                "close": float(data["close"]),
                "volume": int(data["volume"]),
                "date": data["date"],
                "time": data["time"],
                "moving_average": (
                    float(data["moving_average"])
                    if data["moving_average"] is not None
                    else None
                ),
                "cumulative_average": (
                    float(data["cumulative_average"])
                    if data["cumulative_average"] is not None
                    else None
                ),
            }

            # Prepare raw data row
            raw_row = {
                "timestamp": timestamp,
                "symbol": symbol,
                "open": float(data["open"]),
                "high": float(data["high"]),
                "low": float(data["low"]),
                "close": float(data["close"]),
                "volume": int(data["volume"]),
            }

            # Buffer the messages
            self.buffer_message(processed_table_id, processed_row)
            self.buffer_message(raw_table_id, raw_row, is_raw=True)

            message.ack()
            logger.info(f"Processed message for {symbol} at {timestamp}")

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            logger.error(f"Message content: {message.data.decode('utf-8')}")
            message.nack()

    def cleanup(self):
        """Flush all remaining buffers before shutdown"""
        for table_id in self.message_buffer.keys():
            self.flush_buffer(table_id)


def main():
    logger.info("Starting BigQuery Loader...")

    while True:
        try:
            loader = BigQueryLoader()
            subscriber = pubsub_v1.SubscriberClient()
            subscription_path = subscriber.subscription_path(
                GCP_CONFIG["PROJECT_ID"], "stock-data-sub"
            )

            streaming_pull_future = subscriber.subscribe(
                subscription_path, loader.callback
            )
            logger.info(f"Listening for messages on {subscription_path}")

            streaming_pull_future.result()

        except KeyboardInterrupt:
            logger.info("Stopping the loader...")
            loader.cleanup()
            break
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            logger.info("Restarting loader in 10 seconds...")
            time.sleep(10)


if __name__ == "__main__":
    main()
