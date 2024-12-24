1. First run your existing pipeline to completion:

```bash
Terminal 1: python stocks_pipeline.py
Terminal 2: python bigquery_loader.py
```

Let this finish and collect all your data normally.

2. Then, after it's done, run the Dataflow pipeline:

```bash
python dedup_pipeline.py
```

The Dataflow pipeline will:

- Read from the same Pub/Sub subscription
- Process any new data coming in
- Handle deduplication automatically
- Write clean data to the BigQuery tables

This way, you can:

1. Get all your data collected first with your familiar setup
2. Then switch to the deduplication pipeline for ongoing data collection

When the job is running successfully in Dataflow, to see if it's receiving messages, check the log. The log will show:

1. Job started successfully
2. It's running in Streaming Engine mode
3. Workers are being allocated

To check if it's receiving messages, we need to:

1. Make sure your `stocks_pipeline.py` is running in another terminal to send data
2. Look at the Dataflow monitoring console (the URL was provided in the logs):

```
https://console.cloud.google.com/dataflow/jobs/us-central1/28959?project=stock-data-pipeline
```
