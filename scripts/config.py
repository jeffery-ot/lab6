import logging
import os
from io import StringIO
from datetime import datetime
import boto3

# Global log stream for upload
log_stream = StringIO()
s3_client = boto3.client("s3")

S3_BUCKET = "lab6-presentation"
S3_LOG_PREFIX = "kpi-logs/"

def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),         # Console output
            logging.StreamHandler(log_stream)  # In-memory log capture for S3
        ]
    )

def upload_log_to_s3():
    try:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        key = f"{S3_LOG_PREFIX}kpi_log_{timestamp}.log"
        s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=log_stream.getvalue())
        logging.info(f"Uploaded log to s3://{S3_BUCKET}/{key}")
    except Exception as e:
        logging.error(f"Failed to upload log to S3: {e}")
