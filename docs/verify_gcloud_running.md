1. First, run the code in the Python container:

```bash
# Access Python container
docker-compose run python

# Inside the container run
python stock_pipeline.py
```

2. While this is running, let's verify the data flow by checking each component. Open another terminal and run:

```bash
# Access the gcloud container
docker-compose run gcloud
```

3. In the gcloud container, verify that data is flowing:

```bash
# Check GCS bucket for files
gsutil ls gs://stock-data-pipeline-bucket/raw-data/AMZN/
gsutil ls gs://stock-data-pipeline-bucket/processed-data/AMZN/

# Check Pub/Sub messages
gcloud pubsub subscriptions list
```

4. Run this subscriber in another Python container:

```bash
# Open new terminal
docker-compose run python

# Inside the new Python container
python bigquery_loader.py
```

Now you have:

1. First container: Fetching data and publishing to Pub/Sub
2. Second container: Processing messages and loading to BigQuery

3. To verify everything is working:

```bash
# In gcloud container
bq query --use_legacy_sql=false '
SELECT
  timestamp,
  close,
  volume
FROM stock_market.amazon_stock
ORDER BY timestamp DESC
LIMIT 5
'
```
