import boto3
import json
from urllib.parse import unquote_plus
from datetime import datetime

# AWS Clients
s3 = boto3.client("s3")
stepfunctions = boto3.client("stepfunctions", region_name="us-east-1")

# Constants
STEP_FUNCTION_ARN = "arn:aws:states:us-east-1:648637468459:stateMachine:MyStateMachine-fe007948"
LOG_BUCKET = "lab6-rawdata"
LOG_PREFIX = "lambda-logs"

def lambda_handler(event, context):
    # Prepare JSON log structure
    log_data = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "lambda_request_id": context.aws_request_id,
        "event_received": event,
        "processed_files": [],
        "skipped_files": [],
        "stepfunctions_triggered": [],
        "errors": []
    }

    try:
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = unquote_plus(record['s3']['object']['key'])

            if not key.startswith("landing-zone/") or not key.lower().endswith(".csv"):
                log_data["skipped_files"].append(key)
                continue

            payload = {
                "bucket": bucket,
                "key": key,
                "timestamp": datetime.utcnow().isoformat(),
                "trigger": "s3-upload"
            }

            try:
                response = stepfunctions.start_execution(
                    stateMachineArn=STEP_FUNCTION_ARN,
                    name=f"s3-trigger-{int(datetime.utcnow().timestamp())}",
                    input=json.dumps(payload)
                )

                log_data["processed_files"].append(key)
                log_data["stepfunctions_triggered"].append({
                    "key": key,
                    "executionArn": response['executionArn']
                })

            except Exception as e:
                log_data["errors"].append({
                    "key": key,
                    "error": str(e)
                })

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Lambda completed.",
                "processed_files": log_data["processed_files"],
                "skipped_files": log_data["skipped_files"],
                "errors": log_data["errors"]
            })
        }

    finally:
        write_json_log_to_s3(log_data, context)

def write_json_log_to_s3(log_data, context):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    log_key = f"{LOG_PREFIX}/lambda-log-{timestamp}-{context.aws_request_id}.json"

    try:
        log_body = json.dumps(log_data, indent=2)
        s3.put_object(Bucket=LOG_BUCKET, Key=log_key, Body=log_body.encode("utf-8"))
        print(f"Log uploaded to s3://{LOG_BUCKET}/{log_key}")
    except Exception as e:
        print(f"Failed to upload log to S3: {e}") it seems my lambda function triggers per the number of files uploaded to my s3