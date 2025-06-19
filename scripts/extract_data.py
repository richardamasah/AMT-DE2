import os
import boto3
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_s3_data(bucket_name, prefix):
    """Extract data from S3 bucket."""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
        logging.info(f"Listing objects in s3://{bucket_name}/{prefix}")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        files = [obj['Key'] for obj in response.get('Contents', [])]
        logging.info(f"Found files: {files}")
        return files
    except Exception as e:
        logging.error(f"Error extracting from S3: {e}")
        raise