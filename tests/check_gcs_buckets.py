from google.cloud import storage

client = storage.Client()
bucket = client.bucket("stock-data-pipeline-bucket")
blobs = bucket.list_blobs(prefix="raw-data/AMZN")
for blob in blobs:
    print(blob.name)
