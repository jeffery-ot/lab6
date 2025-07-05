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
    response = s3.list_objects_v2(Bucket=LANDING_BUCKET, Prefix=VALIDATED_PREFIX)
    return [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]

def move_to_archive(key):
    archive_key = key.replace(VALIDATED_PREFIX, ARCHIVE_PREFIX)
    try:
        s3.copy_object(Bucket=LANDING_BUCKET, CopySource={"Bucket": LANDING_BUCKET, "Key": key}, Key=archive_key)
        s3.delete_object(Bucket=LANDING_BUCKET, Key=key)
        logging.info(f"Archived {key} to {archive_key}")
    except Exception as e:
        logging.error(f"Failed to archive {key}: {e}")

def upload_log():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    s3.put_object(Bucket=LANDING_BUCKET, Key=f"{LOG_PREFIX}staging_log_{timestamp}.log", Body=log_stream.getvalue())

# ---------- Delta Merge ----------
def merge_into_delta(source_df, target_path, unique_key, partition_col=None):
    source_df = source_df.withColumn("ingested_at", current_timestamp())
    if partition_col and partition_col in source_df.columns:
        source_df = source_df.withColumn(partition_col, to_date(col(partition_col)))

    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        merge_condition = f"target.{unique_key} = source.{unique_key}"
        target.alias("target").merge(
            source_df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        logging.info(f"Merged data into {target_path}")
    else:
        writer = source_df.write.format("delta").mode("overwrite")
        if partition_col and partition_col in source_df.columns:
            writer = writer.partitionBy(partition_col)
        writer.save(target_path)
        logging.info(f"Initialized Delta table at {target_path}")

# ---------- Schema Detection ----------
def detect_data_type(df_columns: set) -> str:
    for dtype, required_cols in REQUIRED_COLUMNS.items():
        if required_cols.issubset(df_columns):
            return dtype
    return None

# ---------- Stage One File ----------
def stage_file(key):
    try:
        s3_uri = f"s3a://{LANDING_BUCKET}/{key}"
        df = spark.read.option("header", True).csv(s3_uri)
        df = df.select([col.strip() for col in df.columns])
        column_set = set(df.columns)

        data_type = detect_data_type(column_set)
        if not data_type:
            logging.warning(f"Unrecognized schema in file: {key}, skipping...")
            return

        selected_cols = SELECT_COLUMNS[data_type]
        df = df.select(*selected_cols)

        unique_key = UNIQUE_KEYS[data_type]
        target_path = STAGING_PATHS[data_type]
        partition_col = "created_at" if data_type in ["orders", "order_items"] else None

        merge_into_delta(df, target_path, unique_key, partition_col)
        move_to_archive(key)

    except Exception as e:
        logging.error(f"Failed to stage file {key}: {e}")

# ---------- Main ----------
def main():
    files = list_validated_files()
    logging.info(f"Found {len(files)} file(s) for staging")

    for key in files:
        stage_file(key)

    upload_log()

if __name__ == "__main__":
    main()
