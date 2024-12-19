We need to create the required GCP resources first before running the scripts.

1. In the gcloud container, create the required resources:

```bash
# Create Cloud Storage bucket
gsutil mb -l us-central1 gs://stock-data-pipeline-bucket

# Create Pub/Sub topic
gcloud pubsub topics create stock-data

# Create Pub/Sub subscription
gcloud pubsub subscriptions create stock-data-sub --topic stock-data

# Create BigQuery dataset
bq mk stock_market
```

3. Once these resources are created, run the pipeline in this order:

Terminal 1 (Start the subscriber first):

```bash
docker-compose run python
python bigquery_loader.py
```

Terminal 2 (Then start the data pipeline):

```bash
docker-compose run python
python stocks_pipeline.py
```

The errors you might get:

- The Cloud Storage bucket didn't exist
- The Pub/Sub subscription didn't exist

After creating these resources, the pipeline should work correctly. The flow will be:

1. `stocks_pipeline.py` fetches data and publishes to Pub/Sub
2. `bigquery_loader.py` listens for messages and loads them into BigQuery

Remember: Both scripts should be running simultaneously - one to publish data and one to consume it.
