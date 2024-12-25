import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from google.cloud import storage

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.append(project_root)
    print(f"Added to Python path: {project_root}")


client = storage.Client()
bucket = client.bucket("stock-data-pipeline-bucket")
blobs = bucket.list_blobs(prefix="raw-data/AMZN")
for blob in blobs:
    print(blob.name)
