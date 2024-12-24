Functionality to save both the raw JSON and a CSV version.
This setup will:

1. Save three types of files in GCS for each data fetch:

   - `raw-data/{symbol}/{timestamp}.json` - Original JSON from API
   - `raw-data/{symbol}/{timestamp}.csv` - Raw data converted to CSV
   - `processed-data/{symbol}/{timestamp}_processed.csv` - Processed data with additional columns

2. Keep the folder structure in GCS organized:

```
bucket/
├── raw-data/
│   └── AAPL/
│       ├── 20241211_120000.json
│       └── 20241211_120000.csv
└── processed-data/
    └── AAPL/
        └── 20241211_120000_processed.csv
```

To implement this: 3. Create the necessary folders in your GCS bucket:

```bash
gs://<your-bucket>/raw-data/
gs://<your-bucket>/processed-data/
```

This way, we maintain both the raw data exactly as it comes from the API (in JSON) and have clean CSV versions for both raw and processed data.

I'll provide a comprehensive technical overview of your Stock Market Data Pipeline project:

1. Data Storage & Processing:
   ✅ Implemented in GCS:

```
bucket/
├── raw-data/
│   └── {symbol}/
│       └── {timestamp}.json
└── processed-data/
    └── {symbol}/
        └── {timestamp}_processed.csv
```

✅ Implemented in BigQuery:

```
stock_market (dataset)
├── {symbol}_stock (processed data)
│   ├── timestamp, symbol, open, high, low, close, volume
│   └── date, time, moving_average, cumulative_average
└── {symbol}_stock_raw (raw data)
    └── timestamp, open, high, low, close, volume
```

Current Infrastructure:

```python
GCP_CONFIG = {
    "PROJECT_ID": "stock-data-pipeline-444011",
    "BUCKET_NAME": "stock-data-pipeline-bucket",
    "TOPIC_NAME": "stock-data",
    "DATASET_NAME": "stock_market",
}
```

2. **Data Flow**:

```plaintext
Alpha Vantage API → Stock Pipeline → Pub/Sub → BigQuery Loader → BigQuery Tables
                                  → GCS Storage → Data Preprocessor
```

3. **Storage Structure**:

```plaintext
BigQuery Dataset (stock_market):
├── Processed Tables
│   ├── amazon_stock
│   ├── tesla_stock
│   ├── [other stock tables]
└── Raw Tables
    ├── amazon_stock_raw
    ├── tesla_stock_raw
    └── [other raw tables]

GCS Bucket:
├── raw-data/
│   └── {symbol}/
└── processed-data/
    └── {symbol}/
```

## Configuration

```python
Key Settings:
- Project ID: stock-data-pipeline-444011
- Dataset: stock_market
- Interval: 5 minutes
- Stocks: AMZN, TSLA, PFE, JPM, IBM, XOM, KO, AAPL
```
