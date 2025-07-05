import os
import random
import uuid
import boto3
from pathlib import Path
from datetime import datetime

# ---------- Configuration ----------
DATA_DIR = Path("/opt/spark/data")
LANDING_BUCKET = "lab6-rawdata"
LANDING_PREFIX = "landing-zone/"
MAX_UPLOADS = 10
FILE_EXT = ".csv"

# ---------- AWS S3 Client ----------
s3 = boto3.client("s3")

# ---------- Helpers ----------
def get_all_csv_files(data_root):
    return [f for f in data_root.rglob(f"*{FILE_EXT}") if f.is_file()]

def make_unique_filename(file_path):
    base_name = file_path.stem  # without extension
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    uid = uuid.uuid4().hex[:8]
    return f"{base_name}__{timestamp}__{uid}.csv"

def upload_file(file_path, s3_key):
    with open(file_path, "rb") as f:
        s3.upload_fileobj(f, LANDING_BUCKET, s3_key)
    print(f"Uploaded {file_path} -> s3://{LANDING_BUCKET}/{s3_key}")

def simulate_file_drop():
    all_files = get_all_csv_files(DATA_DIR)
    if not all_files:
        print(" No CSV files found to upload.")
        return

    num_files = random.randint(1, min(MAX_UPLOADS, len(all_files)))
    selected_files = random.sample(all_files, k=num_files)

    print(f"\nUploading {num_files} randomly selected file(s):\n")
    for file_path in selected_files:
        new_name = make_unique_filename(file_path)
        s3_key = f"{LANDING_PREFIX}{new_name}"
        upload_file(file_path, s3_key)

# ---------- Entry Point ----------
if __name__ == "__main__":
    simulate_file_drop()
