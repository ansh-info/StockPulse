import logging
import time

from google.cloud import bigquery

from config import GCP_CONFIG, STOCK_CONFIGS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def remove_duplicates():
    """Remove duplicates from both raw and processed BigQuery tables."""
    client = bigquery.Client()

    for symbol, config in STOCK_CONFIGS.items():
        # Process the raw table
        raw_table_id = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}.{config['table_name']}_raw"
        raw_dedup_query = f"""
        CREATE OR REPLACE TABLE `{raw_table_id}` AS
        WITH RankedRecords AS (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol, timestamp
                    ORDER BY timestamp DESC
                ) as rn
            FROM `{raw_table_id}`
        )
        SELECT 
            timestamp,
            symbol,
            open,
            high,
            low,
            close,
            volume
        FROM RankedRecords
        WHERE rn = 1
        ORDER BY timestamp DESC;
        """

        # Process the processed table
        processed_table_id = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}.{config['table_name']}"
        processed_dedup_query = f"""
        CREATE OR REPLACE TABLE `{processed_table_id}` AS
        WITH RankedRecords AS (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol, timestamp
                    ORDER BY timestamp DESC
                ) as rn
            FROM `{processed_table_id}`
        )
        SELECT 
            timestamp,
            symbol,
            open,
            high,
            low,
            close,
            volume,
            date,
            time,
            moving_average,
            cumulative_average
        FROM RankedRecords
        WHERE rn = 1
        ORDER BY timestamp DESC;
        """

        try:
            # Process raw table
            logger.info(f"Removing duplicates from {symbol} raw table...")
            query_job = client.query(raw_dedup_query)
            query_job.result()

            # Get count for raw table
            count_query = f"SELECT COUNT(*) as count FROM `{raw_table_id}`"
            count_job = client.query(count_query)
            raw_count = next(count_job.result()).count
            logger.info(f"Completed {symbol} raw table: Now has {raw_count} rows")

            # Process processed table
            logger.info(f"Removing duplicates from {symbol} processed table...")
            query_job = client.query(processed_dedup_query)
            query_job.result()

            # Get count for processed table
            count_query = f"SELECT COUNT(*) as count FROM `{processed_table_id}`"
            count_job = client.query(count_query)
            processed_count = next(count_job.result()).count
            logger.info(
                f"Completed {symbol} processed table: Now has {processed_count} rows"
            )

        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")


def continuous_dedup_check():
    """Continuously check and remove duplicates."""
    while True:
        try:
            logger.info("Starting duplicate removal process...")
            remove_duplicates()
            logger.info(
                "Duplicate removal complete! Waiting for 5 minutes before next check..."
            )
            time.sleep(300)  # Wait for 5 minutes before next check
        except KeyboardInterrupt:
            logger.info("Stopping duplicate removal process...")
            break
        except Exception as e:
            logger.error(f"Error in continuous check: {e}")
            logger.info("Retrying in 1 minute...")
            time.sleep(60)


if __name__ == "__main__":
    continuous_dedup_check()
