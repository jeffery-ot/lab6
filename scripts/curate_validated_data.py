import os
import logging
from datetime import datetime
import pandas as pd
import boto3
from io import StringIO

# ---------- Configuration ----------
VALIDATED_PREFIX = "validated/"
ARCHIVE_PREFIX = "archives/"
TARGET_PREFIX = {
    "products": "staged-products/",
    "orders": "staged-orders/",
    "order_items": "staged-orders/"
}
REQUIRED_COLUMNS = {
    "products": ["id", "category"],
    "orders": ["order_id", "user_id", "status", "created_at", "num_of_item"],
    "order_items": ["id", "order_id", "user_id", "product_id", "status", "created_at", "sale_price"]
}
BUCKET = "lab6-rawdata"
CURATED_BUCKET = "lab6-curated"

# ---------- Initialize ----------
s3 = boto3.client("s3")
log_stream = StringIO()
logging.basicConfig(stream=log_stream, level=logging.INFO)

def list_validated_files():
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=VALIDATED_PREFIX)
    return [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]

def archive_file(key: str):
    archive_key = key.replace(VALIDATED_PREFIX, ARCHIVE_PREFIX, 1)
    try:
        s3.copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": key}, Key=archive_key)
        s3.delete_object(Bucket=BUCKET, Key=key)
        logging.info(f"Archived validated file to s3://{BUCKET}/{archive_key}")
    except Exception as e:
        logging.error(f"Failed to archive file {key}: {e}")

def process_and_save_parquet(file_key: str, data_type: str):
    try:
        s3_obj = s3.get_object(Bucket=BUCKET, Key=file_key)
        df = pd.read_csv(s3_obj["Body"])
        df.columns = [c.strip() for c in df.columns]
        df = df[[col for col in REQUIRED_COLUMNS[data_type] if col in df.columns]]

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = os.path.basename(file_key).replace(".csv", "")
        
        if data_type == "products":
            for category, group_df in df.groupby("category"):
                filename = f"{base_name}_{timestamp}.parquet"
                partition_key = os.path.join(TARGET_PREFIX[data_type], f"category={category}", filename)
                temp_file = "/tmp/temp.parquet"
                group_df.to_parquet(temp_file, index=False, engine="pyarrow")
                with open(temp_file, "rb") as f:
                    s3.upload_fileobj(f, CURATED_BUCKET, partition_key)
                logging.info(f"Saved partitioned product file to s3://{CURATED_BUCKET}/{partition_key}")
        else:
            filename = f"{base_name}_{timestamp}.parquet"
            target_key = os.path.join(TARGET_PREFIX[data_type], filename)
            temp_file = "/tmp/temp.parquet"
            df.to_parquet(temp_file, index=False, engine="pyarrow")
            with open(temp_file, "rb") as f:
                s3.upload_fileobj(f, CURATED_BUCKET, target_key)
            logging.info(f"Saved curated file to s3://{CURATED_BUCKET}/{target_key}")

        # Archive only after successful write
        archive_file(file_key)

    except Exception as e:
        logging.error(f"Failed to process {file_key}: {e}")

def main():
    files = list_validated_files()
    logging.info(f"Found {len(files)} validated files")
    for key in files:
        data_type = next((k for k in REQUIRED_COLUMNS if k in key), None)
        if not data_type:
            logging.warning(f"Skipping file with unknown type: {key}")
            continue
        process_and_save_parquet(key, data_type)

    # Upload logs to curated logs
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"curation_log_{timestamp}.log"
    s3.put_object(Bucket=CURATED_BUCKET, Key=f"logs/{log_filename}", Body=log_stream.getvalue())

if __name__ == "__main__":
    main()
