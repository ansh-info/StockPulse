The issue - the service account doesn't have sufficient permissions to enable APIs.

1. First, go to GCP Console in your browser (https://console.cloud.google.com/):

   - Select your project "stock-data-pipeline-444011"
   - Go to "APIs & Services" → "Library"
   - Enable these APIs manually:
     - Cloud Pub/Sub API
     - Cloud Storage API
     - BigQuery API
     - Cloud Resource Manager API

2. Then, we need to give the service account proper permissions:

   - Go to "IAM & Admin" → "IAM"
   - Find your service account (stock-pipeline-sa@stock-data-pipeline-444011.iam.gserviceaccount.com)
   - Click the edit (pencil) icon
   - Add these roles:
     - BigQuery Admin
     - Storage Admin
     - Pub/Sub Publisher
     - Pub/Sub Subscriber

3. After doing this, try in your gcloud container:

```bash
# Verify project setting
gcloud config get-value project

# Try creating resources
gcloud pubsub topics create stock-data
gsutil mb gs://stock-data-pipeline-bucket
bq mk --dataset stock_market
```

If you still get permission errors, we might need to add additional roles to your service account:

- Service Usage Viewer
- Service Usage Consumer
