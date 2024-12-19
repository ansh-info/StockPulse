1. First, let's verify our GCP setup in the gcloud container:

```bash
# Access the gcloud container
docker-compose run gcloud

# Inside container, authenticate and set project
gcloud auth activate-service-account --key-file=/workspace/keys/key.json
gcloud config set project YOUR_PROJECT_ID

# Create required GCP resources if not already created
gcloud services enable pubsub.googleapis.com
gcloud services enable storage-component.googleapis.com
gcloud services enable bigquery.googleapis.com

# Create Pub/Sub topic
gcloud pubsub topics create stock-data

# Create Cloud Storage bucket
gsutil mb gs://your-project-stock-data

# Create BigQuery dataset
bq mk --dataset stock_market
```

2. Access the Python container and install requirements:

```bash
# Access Python container
docker-compose run python

# Inside container
pip install -r requirements.txt
```

3. Create a BigQuery table (in gcloud container):

```bash
bq mk --table \
    stock_market.amazon_stock \
    timestamp:TIMESTAMP,symbol:STRING,open:FLOAT,high:FLOAT,low:FLOAT,close:FLOAT,volume:INTEGER
```
