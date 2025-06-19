# scripts/extract_load_s3.py (place in my_music_etl_project/scripts/)

import pandas as pd
import boto3
import io
import logging
import os
from typing import Union # NEW: Import Union for type hinting in Python < 3.10

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

def extract_csv_from_s3(s3_key: str) -> Union[pd.DataFrame, None]: # FIXED: Changed | to Union[]
    """
    Extracts a CSV file from the specified S3 key into a Pandas DataFrame.

    Args:
        s3_key (str): The full S3 key (path) to the CSV file (e.g., 'static/users.csv').

    Returns:
        Union[pd.DataFrame, None]: A Pandas DataFrame if the CSV is successfully extracted,
                                 or None if the S3 object (file) is not found.

    Raises:
        Exception: For any other errors during S3 interaction or CSV reading.
    """
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        logger.info(f"Successfully extracted {len(df)} rows from s3://{S3_BUCKET_NAME}/{s3_key}")
        return df
    except s3.exceptions.NoSuchKey:
        logger.warning(f"S3 object not found: s3://{S3_BUCKET_NAME}/{s3_key}. Returning None.")
        return None
    except Exception as e:
        logger.error(f"Error extracting s3://{S3_BUCKET_NAME}/{s3_key}: {e}")
        raise

def upload_dataframe_to_s3_parquet(df: pd.DataFrame, s3_key: str):
    """
    Uploads a Pandas DataFrame to S3 as a Parquet file.

    Args:
        df (pd.DataFrame): The DataFrame to upload.
        s3_key (str): The full S3 key (path) where the Parquet file will be stored
                      (e.g., 'processed/users_dim/users_dim.parquet').

    Raises:
        Exception: For any errors during Parquet conversion or S3 upload.
    """
    s3 = boto3.client('s3')
    try:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=buffer.getvalue())
        logger.info(f"Successfully uploaded {len(df)} rows to s3://{S3_BUCKET_NAME}/{s3_key} as Parquet.")
    except Exception as e:
        logger.error(f"Error uploading DataFrame to s3://{S3_BUCKET_NAME}/{s3_key}: {e}")
        raise
