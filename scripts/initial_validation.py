import os
import logging
from datetime import datetime
import pandas as pd
from io import StringIO
import boto3

# ---------- S3 Configuration ----------
LANDING_BUCKET = "lab6-rawdata"
LANDING_PREFIX = "landing-zone/"
VALIDATED_PREFIX = "validated/"
REJECTED_PREFIX = "rejected/"
QUARANTINED_PREFIX = "quarantined/"
LOG_PREFIX = "validation-logs/"

# ---------- Schema Definitions ----------
REQUIRED_COLUMNS = {
    "products": ["id", "category"],
    "orders": ["order_id", "user_id", "status", "created_at", "returned_at", "num_of_item"],
    "order_items": ["id", "order_id", "user_id", "product_id", "status", "created_at", "sale_price"]
}

CHUNK_SIZE = 10000
log_stream = StringIO()
logging.basicConfig(stream=log_stream, level=logging.INFO)
s3 = boto3.client("s3")

# ---------- Utility Functions ----------

def list_files(bucket, prefix):
    return [obj["Key"] for obj in s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", []) if obj["Key"].endswith(".csv")]

def move_file(key, dest_prefix):
    dest_key = key.replace(LANDING_PREFIX, dest_prefix)
    s3.copy_object(Bucket=LANDING_BUCKET, CopySource={"Bucket": LANDING_BUCKET, "Key": key}, Key=dest_key)
    s3.delete_object(Bucket=LANDING_BUCKET, Key=key)

def upload_log_to_s3(log_content, filename):
    s3.put_object(Bucket=LANDING_BUCKET, Key=f"{LOG_PREFIX}{filename}", Body=log_content)

# ---------- Validation Function ----------

def validate_chunk(df: pd.DataFrame, data_type: str, chunk_idx: int, file_key: str):
    df = df[[col for col in REQUIRED_COLUMNS[data_type] if col in df.columns]].copy()  # <-- add .copy() here

    # Convert to expected types
    if 'created_at' in df.columns:
        df.loc[:, 'created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    if 'returned_at' in df.columns:
        df.loc[:, 'returned_at'] = pd.to_datetime(df['returned_at'], errors='coerce')
    
    for col in ['id', 'order_id', 'user_id', 'product_id', 'num_of_item']:
        if col in df.columns:
            df.loc[:, col] = pd.to_numeric(df[col], errors='coerce')

    if 'sale_price' in df.columns:
        df.loc[:, 'sale_price'] = pd.to_numeric(df['sale_price'], errors='coerce')

    # Separate valid/invalid rows
    invalid_rows = df[df.isnull().any(axis=1)]
    valid_rows = df.dropna()

    if not invalid_rows.empty:
        quarantine_key = f"{QUARANTINED_PREFIX}{data_type}/{os.path.basename(file_key).replace('.csv','')}_chunk{chunk_idx}.csv"
        csv_buffer = StringIO()
        invalid_rows.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=LANDING_BUCKET, Key=quarantine_key, Body=csv_buffer.getvalue())
        logging.warning(f"Chunk {chunk_idx}: {len(invalid_rows)} row(s) quarantined to s3://{LANDING_BUCKET}/{quarantine_key}")

    if valid_rows.empty:
        logging.warning(f"Chunk {chunk_idx} of {file_key} has no valid rows")
        return False

    return True


# ---------- Batch Processing ----------

def process_file_in_chunks(file_key: str, data_type: str):
    s3_obj = s3.get_object(Bucket=LANDING_BUCKET, Key=file_key)
    chunk_iter = pd.read_csv(s3_obj['Body'], chunksize=CHUNK_SIZE)
    all_chunks_valid = True
    for i, chunk in enumerate(chunk_iter):
        chunk.columns = [c.strip() for c in chunk.columns]
        success = validate_chunk(chunk, data_type, i, file_key)
        if not success:
            all_chunks_valid = False
    return all_chunks_valid

# ---------- Main Function ----------

def main():
    files = list_files(LANDING_BUCKET, LANDING_PREFIX)
    logging.info(f"Found {len(files)} file(s) in landing zone")

    for key in files:
        try:
            logging.info(f"Processing file: {key}")
            data_type = next((k for k in REQUIRED_COLUMNS if k in key), None)
            if not data_type:
                logging.warning(f"Unknown file type: {key}")
                move_file(key, REJECTED_PREFIX)
                continue

            valid = process_file_in_chunks(key, data_type)
            if valid:
                move_file(key, VALIDATED_PREFIX)
                logging.info(f"Validation passed : {key}")
            else:
                move_file(key, REJECTED_PREFIX)
                logging.warning(f"Validation failed : {key}")
        except Exception as e:
            logging.error(f"Error processing {key}: {e}")
            move_file(key, REJECTED_PREFIX)

    # Upload log to S3
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"validation_log_{timestamp}.log"
    upload_log_to_s3(log_stream.getvalue(), log_filename)

if __name__ == "__main__":
    main()
