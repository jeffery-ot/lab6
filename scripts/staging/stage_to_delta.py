import os
import logging
from io import StringIO
from datetime import datetime
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp
from delta.tables import DeltaTable

# ---------- Configuration ----------
LANDING_BUCKET = "lab6-rawdata"
VALIDATED_PREFIX = "validated/"
ARCHIVE_PREFIX = "staged/"
LOG_PREFIX = "staging-logs/"

STAGING_PATHS = {
    "orders": "s3a://lab6-curated/staged-orders",
    "order_items": "s3a://lab6-curated/staged-order-items",
    "products": "s3a://lab6-curated/staged-products"
}

UNIQUE_KEYS = {
    "orders": "order_id",
    "order_items": "id",
    "products": "id"
}

REQUIRED_COLUMNS = {
    "products": {"id", "category", "sku"},
    "orders": {"order_id", "user_id", "status", "created_at", "num_of_item"},
    "order_items": {"id", "order_id", "user_id", "product_id", "status", "created_at", "sale_price"}
}

SELECT_COLUMNS = {
    "products": ["id", "category", "sku"],
    "orders": ["order_id", "user_id", "status", "created_at", "num_of_item"],
    "order_items": ["id", "order_id", "user_id", "product_id", "status", "created_at", "sale_price"]
}

# ---------- Initialize ----------
spark = SparkSession.builder \
    .appName("DeltaStagingWithMerge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
s3 = boto3.client("s3")

# ---------- S3 Helpers ----------
def list_validated_files():
    try:
        response = s3.list_objects_v2(Bucket=LANDING_BUCKET, Prefix=VALIDATED_PREFIX)
        files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]
        logger.info(f"Found {len(files)} CSV files in {LANDING_BUCKET}/{VALIDATED_PREFIX}")
        return files
    except Exception as e:
        logger.error(f"Error listing files: {e}")
        return []

def move_to_archive(key):
    archive_key = key.replace(VALIDATED_PREFIX, ARCHIVE_PREFIX)
    try:
        s3.copy_object(Bucket=LANDING_BUCKET, CopySource={"Bucket": LANDING_BUCKET, "Key": key}, Key=archive_key)
        s3.delete_object(Bucket=LANDING_BUCKET, Key=key)
        logger.info(f"Archived {key} to {archive_key}")
    except Exception as e:
        logger.error(f"Failed to archive {key}: {e}")

def upload_log():
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_key = f"{LOG_PREFIX}staging_log_{timestamp}.log"
        s3.put_object(Bucket=LANDING_BUCKET, Key=log_key)
        logger.info(f"Log uploaded to {LANDING_BUCKET}/{log_key}")
    except Exception as e:
        logger.error(f"Failed to upload log: {e}")

# ---------- Delta Merge ----------
def merge_into_delta(source_df, target_path, unique_key, partition_col=None):
    try:
        logger.info(f"Starting merge operation for {target_path}")
        source_df = source_df.withColumn("ingested_at", current_timestamp())

        if partition_col and partition_col in source_df.columns:
            partition_date_col = f"{partition_col}_date"
            logger.debug(f"Creating partition column {partition_date_col} from {partition_col}")
            source_df = source_df.withColumn(partition_date_col, to_date(col(partition_col)))
        else:
            partition_date_col = None

        if DeltaTable.isDeltaTable(spark, target_path):
            logger.info(f"Delta table exists at {target_path}, merging...")
            target = DeltaTable.forPath(spark, target_path)
            merge_condition = f"target.{unique_key} = source.{unique_key}"
            target.alias("target").merge(
                source_df.alias("source"),
                merge_condition
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            logger.info(f"Merge completed for {target_path}")
        else:
            logger.info(f"No Delta table at {target_path}, creating new one...")
            writer = source_df.write.format("delta").mode("overwrite")
            if partition_date_col:
                writer = writer.partitionBy(partition_date_col)
            writer.save(target_path)
            logger.info(f"New Delta table created at {target_path}")
    except Exception as e:
        logger.error(f"Error in merge_into_delta: {e}", exc_info=True)
        raise

# ---------- Schema Detection ----------
def detect_data_type(df_columns: set) -> str:
    logger.debug(f"Detecting data type for columns: {sorted(df_columns)}")
    for dtype, required_cols in REQUIRED_COLUMNS.items():
        if required_cols.issubset(df_columns):
            logger.debug(f" Matched {dtype}")
            return dtype
        else:
            missing = required_cols - df_columns
            logger.debug(f" {dtype}: missing {missing}")
    logger.warning("No matching data type found")
    return None

# ---------- Stage One File ----------
def stage_file(key):
    try:
        logger.info(f"Processing file: {key}")
        s3_uri = f"s3a://{LANDING_BUCKET}/{key}"
        logger.debug(f"Reading CSV from: {s3_uri}")
        
        df = spark.read.option("header", True).csv(s3_uri)
        cleaned_columns = [c.strip() for c in df.columns]
        df = df.select([col(c).alias(c.strip()) for c in df.columns])
        column_set = set(cleaned_columns)
        logger.debug(f"Cleaned columns: {sorted(column_set)}")

        data_type = detect_data_type(column_set)
        if not data_type:
            logger.warning(f"Unrecognized schema in file: {key}, skipping...")
            return

        selected_cols = SELECT_COLUMNS[data_type]
        missing_cols = set(selected_cols) - column_set
        if missing_cols:
            logger.error(f"Missing required columns {missing_cols} in file {key}")
            return
            
        df = df.select(*selected_cols)
        logger.info(f"{key}: {df.count()} rows, {len(df.columns)} columns")

        unique_key = UNIQUE_KEYS[data_type]
        target_path = STAGING_PATHS[data_type]
        partition_col = "created_at" if data_type in ["orders", "order_items"] else None

        merge_into_delta(df, target_path, unique_key, partition_col)
        move_to_archive(key)
        logger.info(f"Successfully processed: {key}")
    except Exception as e:
        logger.error(f"Error processing {key}: {e}", exc_info=True)

# ---------- Main ----------
def main():
    logger.info("Starting Delta staging process...")
    files = list_validated_files()
    logger.info(f"Found {len(files)} file(s) for staging")
    
    if not files:
        logger.warning("No files found for processing!")
        return

    for key in files:
        stage_file(key)

    upload_log()
    logger.info("Delta staging process completed!")

if __name__ == "__main__":
    main()
