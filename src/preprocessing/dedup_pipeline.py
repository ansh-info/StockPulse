import logging

from google.cloud import bigquery

from config import GCP_CONFIG, STOCK_CONFIGS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def remove_duplicates():
    """Remove duplicates from BigQuery tables."""
    client = bigquery.Client()

    for symbol, config in STOCK_CONFIGS.items():
        table_id = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}.{config['table_name']}"

        # Simple query to remove duplicates
        dedup_query = f"""
        CREATE OR REPLACE TABLE `{table_id}` AS
        WITH RankedRecords AS (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol, timestamp
                    ORDER BY timestamp DESC
                ) as rn
            FROM `{table_id}`
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
            logger.info(f"Removing duplicates from {symbol} table...")
            query_job = client.query(dedup_query)
            query_job.result()

            # Get count of rows after deduplication
            count_query = f"SELECT COUNT(*) as count FROM `{table_id}`"
            count_job = client.query(count_query)
            count_result = count_job.result()
            row_count = next(count_result).count

            logger.info(f"Completed {symbol}: Table now has {row_count} rows")

        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")


if __name__ == "__main__":
    logger.info("Starting duplicate removal process...")
    remove_duplicates()
    logger.info("Duplicate removal complete!")
