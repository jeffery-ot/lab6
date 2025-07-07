import logging
import os
import boto3
from urllib.parse import urlparse

def configure_logging():
    import io
    from datetime import datetime

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_stream = io.StringIO()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(log_stream)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    s3 = boto3.client("s3")
    log_key = f"kpi-logs/kpi_log_{timestamp}.log"
    s3.put_object(
        Bucket="lab6-presentation",
        Key=log_key,
        Body=log_stream.getvalue()
    )

def ensure_s3_path_exists(s3a_uri):
    """
    Touches the S3 path to ensure it exists before writing (optional in S3, but safe).
    """
    logger = logging.getLogger(__name__)
    parsed = urlparse(s3a_uri.replace("s3a://", "s3://"))
    bucket = parsed.netloc
    prefix = parsed.path.strip("/") + "/_PLACEHOLDER"

    try:
        boto3.client("s3").put_object(Bucket=bucket, Key=prefix, Body=b"")
        logger.info(f"Ensured path exists: s3://{bucket}/{prefix}")
    except Exception as e:
        logger.error(f"Failed to create path: s3://{bucket}/{prefix} - {e}")
