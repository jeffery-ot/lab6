import great_expectations as gx

# Initialize Data Context
context = gx.get_context()

# Add S3 datasource
datasource = context.sources.add_pandas_s3(
    name="s3_landing_zone",
    bucket="s3://lab6-rawdata/landing-zone/",
    boto3_options={
        "region_name": "us-east-1"
    }
)