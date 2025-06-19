import psycopg2
import os
import logging
from typing import Union  # Required for Python < 3.10 for type hints like pd.DataFrame | None
from airflow.hooks.base import BaseHook
import pandas as pd  # Needed for pd.read_sql in read_dimensions_from_redshift

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# AWS S3 details remain as environment variables.
# MWAA's execution role often handles S3 permissions implicitly for boto3,
# but the COPY command explicitly requires access keys.
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# Define the Airflow Connection ID that will be configured in MWAA for Redshift
# Using 'redshift_default' as it worked in your demo DAG and is standard for MWAA
REDSHIFT_CONN_ID = 'redshift_default'


def get_redshift_connection():
    """
    Establishes and returns a connection to Redshift using psycopg2,
    retrieving connection details from an Airflow Connection.
    """
    try:
        # Get connection details from Airflow's metadata database using BaseHook
        conn_data = BaseHook.get_connection(REDSHIFT_CONN_ID)

        # Connect to Redshift using the retrieved details
        conn = psycopg2.connect(
            host=conn_data.host,
            port=conn_data.port,
            # In Airflow Connection, 'schema' field is typically used for the database name
            database=conn_data.schema,
            user=conn_data.login,
            password=conn_data.password
        )
        logger.info(f"Successfully connected to Redshift using Airflow Connection '{REDSHIFT_CONN_ID}'.")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to Redshift via Airflow Connection '{REDSHIFT_CONN_ID}': {e}")
        raise  # Re-raise the exception to fail the Airflow task if connection fails


def load_data_to_redshift_from_s3(
    table_name: str,
    s3_key_prefix: str,
    primary_key_cols: Union[list, None] = None
):
    """
    Loads Parquet data from a specified S3 location into a Redshift table using the COPY command.
    It supports an UPSERT (UPDATE existing, then INSERT new) strategy if primary_key_cols are provided.
    """
    conn = None  # Initialize connection to None for finally block
    cur = None   # Initialize cursor to None for finally block
    try:
        conn = get_redshift_connection()  # Get Redshift connection via Airflow Connection
        cur = conn.cursor()  # Get a cursor to execute SQL commands

        # Create a temporary staging table.
        # This is a best practice for UPSERTs to load data without affecting the main table
        # until the merge is complete. os.urandom(6).hex() creates a unique suffix.
        staging_table_name = f"stg_{table_name}_{os.urandom(6).hex()}"
        cur.execute(f"CREATE TEMP TABLE {staging_table_name} (LIKE {table_name})")  # Create like target table
        logger.info(f"Created staging table: {staging_table_name}")

        # Construct the Redshift COPY command.
        # It reads Parquet files from S3 directly into the staging table.
        # AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are read from MWAA Environment Variables.
        copy_command = f"""
        COPY {staging_table_name}
        FROM 's3://{S3_BUCKET_NAME}/{s3_key_prefix}'
        CREDENTIALS 'aws_access_key_id={AWS_ACCESS_KEY_ID};aws_secret_access_key={AWS_SECRET_ACCESS_KEY}'
        FORMAT AS PARQUET;
        """
        cur.execute(copy_command)
        conn.commit()  # Commit the COPY operation to make data visible in the staging table
        logger.info(f"Data copied to staging table {staging_table_name}. Rows affected: {cur.rowcount}")

        # Perform UPSERT (UPDATE then INSERT) if primary_key_cols are provided.
        if primary_key_cols:
            # Get all column names from the target table to ensure consistent INSERT/UPDATE statements.
            cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position;")
            target_cols = [row[0] for row in cur.fetchall()]
            target_cols_str = ", ".join(target_cols)  # Comma-separated list for SQL queries

            # --- UPDATE Existing Rows ---
            # Create SET clauses for columns that are NOT part of the primary key.
            update_set_clauses = [f"{col} = stg.{col}" for col in target_cols if col not in primary_key_cols]
            update_set_str = ", ".join(update_set_clauses)

            if update_set_str:  # Only attempt update if there are non-primary key columns to update
                update_conditions = " AND ".join([f"{table_name}.{pk} = stg.{pk}" for pk in primary_key_cols])
                update_command = f"""
                UPDATE {table_name}
                SET {update_set_str}
                FROM {staging_table_name} stg
                WHERE {update_conditions}
                """
                cur.execute(update_command)
                logger.info(f"Updated {cur.rowcount} existing rows in {table_name}.")

            # --- INSERT New Rows ---
            # Insert rows from the staging table that do NOT already exist in the target table
            # (based on the primary key).
            insert_conditions = " AND ".join([f"target.{pk} = stg.{pk}" for pk in primary_key_cols])
            insert_command = f"""
            INSERT INTO {table_name} ({target_cols_str})
            SELECT {target_cols_str}
            FROM {staging_table_name} stg
            WHERE NOT EXISTS (
                SELECT 1 FROM {table_name} target
                WHERE {insert_conditions}
            );
            """
            cur.execute(insert_command)
            logger.info(f"Inserted {cur.rowcount} new rows into {table_name}.")

        else:
            # If no primary keys are provided, simply append all data from the staging table to the target.
            # This is simpler but can lead to duplicate rows if run multiple times without a primary key.
            logger.warning(f"No primary key columns provided for table {table_name}. Appending data from staging table. This may lead to duplicates.")
            cur.execute(f"INSERT INTO {table_name} SELECT * FROM {staging_table_name}")
            logger.info(f"Appended {cur.rowcount} rows to {table_name}.")

        conn.commit()  # Commit the entire transaction (UPDATE and INSERT)
        logger.info(f"Data load and upsert for {table_name} completed successfully.")

    except Exception as e:
        logger.error(f"Error loading data to Redshift table {table_name}: {e}")
        if conn:
            conn.rollback()  # Rollback the transaction on error to maintain data consistency
        raise  # Re-raise the exception to propagate the error up to Airflow
    finally:
        # Ensure that the cursor and connection are closed even if an error occurs.
        if cur:
            cur.close()
        if conn:
            conn.close()
            logger.info("Redshift connection closed.")
