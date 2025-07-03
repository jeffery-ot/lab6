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
    "orders": ["order_id", "user_id", "status", "created_at", "num_of_item"],
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
    required_cols = REQUIRED_COLUMNS[data_type]
    df = df[[col for col in required_cols if col in df.columns]].copy()
    conversion_issues = []

    # Parse datetime columns
    for date_col in ['created_at']:
        if date_col in df.columns:
            df.loc[:, date_col] = pd.to_datetime(df[date_col], errors='coerce')
            na_count = df[date_col].isna().sum()
            if na_count > 0:
                conversion_issues.append(f"{na_count} null(s) after datetime parse in column '{date_col}'")

    # Parse numeric columns
    for num_col in ['id', 'order_id', 'user_id', 'product_id', 'num_of_item', 'sale_price']:
        if num_col in df.columns:
            df.loc[:, num_col] = pd.to_numeric(df[num_col], errors='coerce')
            na_count = df[num_col].isna().sum()
            if na_count > 0:
                conversion_issues.append(f"{na_count} null(s) after numeric parse in column '{num_col}'")

    # Detect invalid rows based on required columns only
    invalid_mask = df[required_cols].isnull().any(axis=1)
    invalid_rows = df[invalid_mask]
    valid_rows = df[~invalid_mask]

    if not invalid_rows.empty:
        quarantine_key = f"{QUARANTINED_PREFIX}{data_type}/{os.path.basename(file_key).replace('.csv','')}_chunk{chunk_idx}.csv"
        csv_buffer = StringIO()
        invalid_rows.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=LANDING_BUCKET, Key=quarantine_key, Body=csv_buffer.getvalue())

        reason = "; ".join(conversion_issues) if conversion_issues else "Missing values in required columns"
        logging.warning(
            f"Chunk {chunk_idx}: {len(invalid_rows)} row(s) quarantined to s3://{LANDING_BUCKET}/{quarantine_key} due to: {reason}"
        )

    if valid_rows.empty:
        logging.warning(f"Chunk {chunk_idx} of {file_key} has no valid rows")
        return False

    return True


# ---------- Batch Processing ----------

def process_file_in_chunks(file_key: str, data_type: str):
    try:
        s3_obj = s3.get_object(Bucket=LANDING_BUCKET, Key=file_key)
        chunk_iter = pd.read_csv(s3_obj['Body'], chunksize=CHUNK_SIZE)
        all_chunks_valid = True
        for i, chunk in enumerate(chunk_iter):
            chunk.columns = [c.strip() for c in chunk.columns]
            success = validate_chunk(chunk, data_type, i, file_key)
            if not success:
                all_chunks_valid = False
        return all_chunks_valid
    except Exception as e:
        logging.error(f"Failed to process chunks in {file_key}: {e}")
        return False

# ---------- Main Function ----------

def main():
    files = list_files(LANDING_BUCKET, LANDING_PREFIX)
    logging.info(f"Found {len(files)} file(s) in landing zone")

    for key in files:
        try:
            logging.info(f"Processing file: {key}")
            data_type = next((k for k in REQUIRED_COLUMNS if k in key), None)

            if not data_type:
                logging.warning(f"Rejected file {key}: unknown data type (expected one of {list(REQUIRED_COLUMNS.keys())})")
                move_file(key, REJECTED_PREFIX)
                continue

            valid = process_file_in_chunks(key, data_type)

            if valid:
                move_file(key, VALIDATED_PREFIX)
                logging.info(f"Validation passed : {key}")
            else:
                logging.warning(f"Rejected file {key}: one or more chunks failed validation")
                move_file(key, REJECTED_PREFIX)

        except Exception as e:
            logging.error(f"Error processing file {key}: {e}")
            move_file(key, REJECTED_PREFIX)

    # Upload log to S3
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"validation_log_{timestamp}.log"
    upload_log_to_s3(log_stream.getvalue(), log_filename)

if __name__ == "__main__":
    main()
