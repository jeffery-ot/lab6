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

log_stream = StringIO()
logging.basicConfig(stream=log_stream, level=logging.INFO)
s3 = boto3.client("s3")

# ---------- S3 Helpers ----------
def list_validated_files():
    try:
        response = s3.list_objects_v2(Bucket=LANDING_BUCKET, Prefix=VALIDATED_PREFIX)
        files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]
        print(f"Found {len(files)} CSV files in {LANDING_BUCKET}/{VALIDATED_PREFIX}")
        for f in files:
            print(f"  - {f}")
        return files
    except Exception as e:
        print(f"Error listing files: {e}")
        logging.error(f"Error listing files: {e}")
        return []

def move_to_archive(key):
    archive_key = key.replace(VALIDATED_PREFIX, ARCHIVE_PREFIX)
    try:
        s3.copy_object(Bucket=LANDING_BUCKET, CopySource={"Bucket": LANDING_BUCKET, "Key": key}, Key=archive_key)
        s3.delete_object(Bucket=LANDING_BUCKET, Key=key)
        print(f"Archived {key} to {archive_key}")
        logging.info(f"Archived {key} to {archive_key}")
    except Exception as e:
        print(f"Failed to archive {key}: {e}")
        logging.error(f"Failed to archive {key}: {e}")

def upload_log():
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_key = f"{LOG_PREFIX}staging_log_{timestamp}.log"
        s3.put_object(Bucket=LANDING_BUCKET, Key=log_key, Body=log_stream.getvalue())
        print(f"Log uploaded to {LANDING_BUCKET}/{log_key}")
    except Exception as e:
        print(f"Failed to upload log: {e}")

# ---------- Delta Merge ----------
def merge_into_delta(source_df, target_path, unique_key, partition_col=None):
    try:
        print(f"Starting merge operation for {target_path}")
        source_df = source_df.withColumn("ingested_at", current_timestamp())
        
        if partition_col and partition_col in source_df.columns:
            print(f"Adding partition column: {partition_col}")
            source_df = source_df.withColumn(partition_col, to_date(col(partition_col)))

        print(f"Checking if Delta table exists at {target_path}")
        if DeltaTable.isDeltaTable(spark, target_path):
            print(f"Delta table exists, performing merge...")
            target = DeltaTable.forPath(spark, target_path)
            merge_condition = f"target.{unique_key} = source.{unique_key}"
            target.alias("target").merge(
                source_df.alias("source"),
                merge_condition
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            print(f"Merge completed for {target_path}")
            logging.info(f"Merged data into {target_path}")
        else:
            print(f"Creating new Delta table at {target_path}")
            writer = source_df.write.format("delta").mode("overwrite")
            if partition_col and partition_col in source_df.columns:
                writer = writer.partitionBy(partition_col)
            writer.save(target_path)
            print(f"New Delta table created at {target_path}")
            logging.info(f"Initialized Delta table at {target_path}")
            
    except Exception as e:
        print(f"Error in merge_into_delta: {e}")
        logging.error(f"Error in merge_into_delta: {e}")
        import traceback
        traceback.print_exc()
        raise

# ---------- Schema Detection ----------
def detect_data_type(df_columns: set) -> str:
    print(f"Detecting data type for columns: {sorted(df_columns)}")
    for dtype, required_cols in REQUIRED_COLUMNS.items():
        print(f"  Checking {dtype}: requires {required_cols}")
        if required_cols.issubset(df_columns):
            print(f"  ✓ Matched {dtype}")
            return dtype
        else:
            missing = required_cols - df_columns
            print(f"  ✗ Missing columns for {dtype}: {missing}")
    print("  No data type matched")
    return None

# ---------- Stage One File ----------
def stage_file(key):
    try:
        print(f"\n{'='*60}")
        print(f"Processing file: {key}")
        print(f"{'='*60}")
        
        s3_uri = f"s3a://{LANDING_BUCKET}/{key}"
        print(f"Reading from: {s3_uri}")
        
        df = spark.read.option("header", True).csv(s3_uri)
        print(f"Initial columns: {df.columns}")
        
        # Clean column names and create new dataframe
        cleaned_columns = [c.strip() for c in df.columns]
        df = df.select([col(c).alias(c.strip()) for c in df.columns])
        column_set = set(cleaned_columns)
        print(f"Cleaned columns: {sorted(column_set)}")

        data_type = detect_data_type(column_set)
        if not data_type:
            print(f" Unrecognized schema in file: {key}, skipping...")
            print(f"Available required columns:")
            for dtype, req_cols in REQUIRED_COLUMNS.items():
                print(f"  {dtype}: {req_cols}")
            logging.warning(f"Unrecognized schema in file: {key}, skipping...")
            return

        print(f"✓ Detected data type: {data_type}")
        selected_cols = SELECT_COLUMNS[data_type]
        print(f"Selecting columns: {selected_cols}")
        
        # Check if all required columns exist
        missing_cols = set(selected_cols) - column_set
        if missing_cols:
            print(f" Missing required columns: {missing_cols}")
            logging.error(f"Missing columns {missing_cols} in file {key}")
            return
            
        df = df.select(*selected_cols)
        print(f"DataFrame shape: {df.count()} rows, {len(df.columns)} columns")

        unique_key = UNIQUE_KEYS[data_type]
        target_path = STAGING_PATHS[data_type]
        partition_col = "created_at" if data_type in ["orders", "order_items"] else None

        print(f"Target path: {target_path}")
        print(f"Unique key: {unique_key}")
        print(f"Partition column: {partition_col}")

        merge_into_delta(df, target_path, unique_key, partition_col)
        move_to_archive(key)
        print(f" Successfully processed: {key}")

    except Exception as e:
        print(f" Error processing {key}: {str(e)}")
        logging.error(f"Failed to stage file {key}: {e}")
        import traceback
        traceback.print_exc()

# ---------- Main ----------
def main():
    print("Starting Delta staging process...")
    print(f"Source bucket: {LANDING_BUCKET}")
    print(f"Validated prefix: {VALIDATED_PREFIX}")
    print(f"Target paths: {STAGING_PATHS}")
    print()
    
    files = list_validated_files()
    logging.info(f"Found {len(files)} file(s) for staging")
    
    if not files:
        print("No files found for processing!")
        return

    for key in files:
        stage_file(key)

    upload_log()
    print("\nDelta staging process completed!")

if __name__ == "__main__":
    main()