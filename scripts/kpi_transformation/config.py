import logging
import os
import boto3
from urllib.parse import urlparse
from datetime import datetime
import io
from delta.tables import DeltaTable
from pyspark.sql.functions import col

def configure_logging():
    """Configure logging for the application"""
    # Create timestamp for log file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Configure root logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        handlers=[
            logging.StreamHandler(),  # Console output
        ]
    )
    
    # Create custom handler for S3 logging
    logger = logging.getLogger("pipeline")
    
    # Create string buffer for S3 logging
    log_buffer = io.StringIO()
    s3_handler = logging.StreamHandler(log_buffer)
    s3_handler.setLevel(logging.INFO)
    s3_formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
    s3_handler.setFormatter(s3_formatter)
    
    # Add S3 handler to root logger
    root_logger = logging.getLogger()
    root_logger.addHandler(s3_handler)
    
    # Store buffer reference for later S3 upload
    root_logger.s3_log_buffer = log_buffer
    root_logger.s3_log_timestamp = timestamp
    
    logger.info("Logging configured successfully")

def upload_logs_to_s3():
    """Upload accumulated logs to S3"""
    try:
        root_logger = logging.getLogger()
        if hasattr(root_logger, 's3_log_buffer') and hasattr(root_logger, 's3_log_timestamp'):
            s3_client = boto3.client("s3")
            log_key = f"kpi-logs/kpi_log_{root_logger.s3_log_timestamp}.log"
            
            # Get log content
            log_content = root_logger.s3_log_buffer.getvalue()
            
            if log_content.strip():  # Only upload if there's actual content
                s3_client.put_object(
                    Bucket="lab6-presentation",
                    Key=log_key,
                    Body=log_content
                )
                print(f"Logs uploaded to s3://lab6-presentation/{log_key}")
            else:
                print("No logs to upload")
                
    except Exception as e:
        print(f"Failed to upload logs to S3: {e}")

def ensure_s3_path_exists(s3a_uri):
    """
    Touches the S3 path to ensure it exists before writing (optional in S3, but safe).
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Parse S3 URI
        parsed = urlparse(s3a_uri.replace("s3a://", "s3://"))
        bucket = parsed.netloc
        prefix = parsed.path.strip("/")
        
        # Add placeholder file to ensure path exists
        placeholder_key = f"{prefix}/_PLACEHOLDER" if prefix else "_PLACEHOLDER"
        
        s3_client = boto3.client("s3")
        s3_client.put_object(Bucket=bucket, Key=placeholder_key, Body=b"")
        logger.info(f"Ensured path exists: s3://{bucket}/{placeholder_key}")
        
    except Exception as e:
        logger.error(f"Failed to create path for {s3a_uri}: {e}")
        raise

def cleanup_s3_placeholder(s3a_uri):
    """Remove placeholder file after successful write"""
    logger = logging.getLogger(__name__)
    
    try:
        parsed = urlparse(s3a_uri.replace("s3a://", "s3://"))
        bucket = parsed.netloc
        prefix = parsed.path.strip("/")
        placeholder_key = f"{prefix}/_PLACEHOLDER" if prefix else "_PLACEHOLDER"
        
        s3_client = boto3.client("s3")
        s3_client.delete_object(Bucket=bucket, Key=placeholder_key)
        logger.info(f"Cleaned up placeholder: s3://{bucket}/{placeholder_key}")
        
    except Exception as e:
        logger.warning(f"Failed to cleanup placeholder for {s3a_uri}: {e}")




def archive_to_s3(df, partition_column, output_path, staging_path=None):
    """
    Archive only the partition(s) from staging that were used in df.
    """
    logger = logging.getLogger(__name__)
    try:
        # Step 1: Write to archive path
        df.write.partitionBy(partition_column).mode("overwrite").format("delta").save(output_path)
        logger.info(f"Archived data to {output_path}")

        # Step 2: Delete only affected partitions from staging
        if staging_path:
            partition_values = df.select(partition_column).distinct().rdd.flatMap(lambda x: x).collect()
            if not partition_values:
                logger.warning(f"No partition values found in DataFrame for {partition_column}")
                return

            delta_table = DeltaTable.forPath(df._jdf.sparkSession(), staging_path)

            for val in partition_values:
                logger.info(f"Deleting partition {partition_column} = {val} from {staging_path}")
                delta_table.delete(condition=col(partition_column) == val)

            logger.info(f"Deleted {len(partition_values)} partition(s) from staging path {staging_path}")

    except Exception as e:
        logger.error(f"Failed to archive and clean staging: {e}")

