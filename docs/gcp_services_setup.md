1. First, set up GCP Project and required services:

```bash
# Enable required APIs in your GCP project
gcloud services enable pubsub.googleapis.com
gcloud services enable storage-component.googleapis.com
gcloud services enable bigquery.googleapis.com
```

GCP from scratch, including using Google Cloud SDK in Docker.

1. GCP Browser Setup:

a. Create GCP Account & Project:

- Go to https://console.cloud.google.com/
- Sign up with your Google account
- Click "Create Project" in the top bar
- Name your project (e.g., "stock-data-pipeline")
- Note down the Project ID (it's different from project name)

b. Enable Billing:

- Go to Billing in the navigation menu
- Link a billing account (required for using GCP services)
- Can set up billing alerts here to avoid unexpected charges

c. Enable Required APIs:

- Go to "APIs & Services" → "Library"
- Search and enable these APIs:
  - Cloud Pub/Sub API
  - Cloud Storage API
  - BigQuery API
  - Cloud Functions API
  - App Engine API

d. Create Service Account:

- Go to "IAM & Admin" → "Service Accounts"
- Click "Create Service Account"
- Name: "stock-pipeline-sa"
- Grant these roles:
  - BigQuery Admin
  - Storage Admin
  - Pub/Sub Publisher
  - Pub/Sub Subscriber
- Click "Create Key" (JSON format)
- Download the JSON key file
- Save it securely (never commit to git)

2. Docker Setup for Google Cloud SDK:

a. Create a new directory for your project:

```bash
mkdir stock-pipeline
cd stock-pipeline
```

b. Create a Dockerfile:

```dockerfile
FROM google/cloud-sdk:latest

# Install Python and pip
RUN apt-get update && apt-get install -y \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install required Python packages
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Set working directory
WORKDIR /app

# Copy service account key
COPY key.json /app/key.json

# Set environment variable for authentication
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/key.json

# Keep container running
CMD ["bash"]
```

c. Create requirements.txt:

```txt
google-cloud-pubsub
google-cloud-storage
google-cloud-bigquery
pandas
requests
schedule
streamlit
```

d. Create a docker-compose.yml:

```yaml
version: "3"
services:
  cloud-sdk:
    build: .
    volumes:
      - .:/app
    environment:
      - GOOGLE_APPLICATION_CREDENTIALS=/app/key.json
    ports:
      - "8501:8501" # for Streamlit
```

3. Setup Steps:

a. Prepare your environment:

```bash
# Create project directory structure
mkdir -p stock-pipeline/{data,src,config}
cd stock-pipeline

# Move your service account key
mv ~/Downloads/your-key.json ./key.json
```

b. Build and run Docker container:

```bash
# Build the container
docker-compose build

# Run the container
docker-compose up -d

# Access the container
docker-compose exec cloud-sdk bash
```

c. Inside the container, authenticate with GCP:

```bash
# Activate service account
gcloud auth activate-service-account --key-file=/app/key.json

# Set project ID
gcloud config set project YOUR_PROJECT_ID

# Verify configuration
gcloud config list
```

4. Create GCP Resources:

Inside the container, run these commands:

```bash
# Create Cloud Storage bucket
gsutil mb -l us-central1 gs://your-project-stock-data

# Create Pub/Sub topic
gcloud pubsub topics create stock-data

# Create Pub/Sub subscription
gcloud pubsub subscriptions create stock-data-sub --topic stock-data

# Create BigQuery dataset
bq mk --dataset stock_data
```

5. Test Setup:

```bash
# Test Cloud Storage
echo "test" > test.txt
gsutil cp test.txt gs://your-project-stock-data/

# Test BigQuery
bq query --use_legacy_sql=false 'SELECT 1'
```
