# dags/music_etl_pipeline.py (place in my_music_etl_project/dags/)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta
import logging
import os

# Import the functions from your scripts folder.
# The 'scripts/' folder is explicitly added to the Python path via docker-compose.yaml's PYTHONPATH.
# This ensures that Python within the Airflow containers can find these modules.
from scripts.data_transformation import load_static_data_to_redshift_task, process_stream_data_task

# Configure logging for the DAG file.
# This allows logs from your DAG's top-level code to be visible in Airflow and Docker logs.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# --- Initial Static Data Load DAG ---
# This DAG is designed to run ONCE to populate your dimension tables in Redshift.
# You can trigger it manually after setting up your Redshift tables and uploading static CSVs to S3.
with DAG(
    dag_id='initial_static_data_load_dag',
    start_date=days_ago(1),  # Set a start date in the past for immediate scheduling
    schedule_interval=None,  # Set to None for manual trigger (recommended for initial loads)
    catchup=False,           # Do not perform backfills for past missed schedules
    tags=['etl', 'initial-load', 'redshift'], # Tags for easy filtering and organization in Airflow UI
    doc_md="""
    ### Initial Static Data Load DAG
    This DAG is responsible for the one-time loading of static user and song metadata
    from S3 into Redshift dimension tables (users_dim, songs_dim).
    It should be triggered manually after setting up the Redshift tables and uploading
    the 'users.csv' and 'songs.csv' files to S3.
    """
) as initial_static_data_load_dag:
    
    # Define a PythonOperator task that calls the 'load_static_data_to_redshift_task' function
    # from your 'etl_processor' script.
    load_static_task = PythonOperator(
        task_id='load_static_users_songs_to_redshift', # Unique identifier for the task
        python_callable=load_static_data_to_redshift_task, # The Python function to execute
        do_xcom_push=False, # Set to False as this task's return value isn't needed by downstream tasks
    )

# --- Stream ETL Pipeline DAG ---
# This DAG will run incrementally, processing stream files from S3.
# It uses Airflow Variables to keep track of the last processed stream file, enabling sequential processing.

# Define the list of stream files to be processed in order.
# Ensure these filenames match the CSV files you uploaded to your S3 'streams/' prefix.
STREAM_FILES = ['stream1.csv', 'stream2.csv', 'stream3.csv']

# Airflow Variable key to store the index of the last processed stream file.
# This variable helps the DAG maintain state across runs.
# '0' would mean 'stream1.csv', '1' means 'stream2.csv', etc.
# The initial value should be -1 if no stream files have been processed yet.
LAST_PROCESSED_STREAM_VAR_KEY = 'last_processed_stream_index'

with DAG(
    dag_id='stream_etl_pipeline_dag',
    start_date=days_ago(1),           # Set a start date in the past
    schedule_interval=timedelta(hours=6), # Example: Schedule to run every 6 hours
    catchup=False,                    # Do not backfill past missed schedules
    tags=['etl', 'streaming', 'redshift'], # Tags for UI filtering
    doc_md="""
    ### Stream ETL Pipeline DAG
    This DAG processes stream data incrementally from S3, validates, transforms,
    computes KPIs using Pandas, and loads them to Redshift Serverless.
    It processes stream files sequentially (stream1, then stream2, then stream3)
    and handles cases where no new file is available gracefully.
    This DAG requires the 'initial_static_data_load_dag' to be run successfully first
    to populate the dimension tables in Redshift.
    """
) as stream_etl_pipeline_dag:

    def _get_next_stream_file(**context):
        """
        Determines the next stream file to process based on the 'last_processed_stream_index' Airflow Variable.
        Pushes the selected filename and its index to Airflow's XComs (cross-communication mechanism)
        for downstream tasks to retrieve. Returns None if all defined files have been processed.
        """
        # Retrieve the current index from the Airflow Variable. Default to "-1" if not found.
        last_processed_index = Variable.get(LAST_PROCESSED_STREAM_VAR_KEY, default="-1", deserialize_json=False)
        last_processed_index = int(last_processed_index) # Convert the string value to an integer

        next_index = last_processed_index + 1 # Calculate the index of the next file to process
        
        # Check if there are more stream files to process based on our predefined list
        if next_index < len(STREAM_FILES):
            next_file = STREAM_FILES[next_index]
            logger.info(f"Next stream file to attempt processing: {next_file}")
            # Use XComs to make the filename and index available to other tasks in the DAG run
            context['ti'].xcom_push(key='next_stream_file', value=next_file)
            context['ti'].xcom_push(key='next_stream_index', value=next_index)
        else:
            logger.info("All defined stream files have been processed. No new stream file to process.")
            # If no more files, push None to indicate completion.
            context['ti'].xcom_push(key='next_stream_file', value=None)
            context['ti'].xcom_push(key='next_stream_index', value=None)

    # Define a PythonOperator task to get the next stream file
    get_next_stream_task = PythonOperator(
        task_id='get_next_stream_file',
        python_callable=_get_next_stream_file,
        provide_context=True, # Essential for accessing XComs and Airflow context
    )

    def _process_and_update_state(**context):
        """
        Retrieves the stream file name from XCom, calls the main ETL processing function,
        and updates the 'last_processed_stream_index' Airflow Variable upon successful completion.
        """
        # Pull the stream file name and its index from the XComs pushed by the previous task
        stream_file_to_process = context['ti'].xcom_pull(key='next_stream_file', task_ids='get_next_stream_file')
        next_index_to_update = context['ti'].xcom_pull(key='next_stream_index', task_ids='get_next_stream_file')

        if stream_file_to_process:
            logger.info(f"Attempting to process stream file: {stream_file_to_process}")
            try:
                # Call the core ETL processing function from scripts.etl_processor.
                # This function handles extraction, validation, transformation, and loading.
                process_stream_data_task(stream_file_name=stream_file_to_process)
                # If processing is successful, update the Airflow Variable to reflect the new state.
                Variable.set(LAST_PROCESSED_STREAM_VAR_KEY, str(next_index_to_update), serialize_json=False)
                logger.info(f"Successfully processed {stream_file_to_process} and updated last_processed_stream_index to {next_index_to_update}.")
            except Exception as e:
                logger.error(f"Failed to process {stream_file_to_process}: {e}")
                # Re-raise the exception. This will cause the Airflow task to fail,
                # allowing for retries or alerting.
                raise
        else:
            logger.info("No new stream file identified for processing. Task finished gracefully.")
            # If no file was processed (e.g., all streams are done), do not update the Variable.
            # This ensures that if new stream files are added later, the DAG can pick up from where it left off.

    # Define a PythonOperator task to process the current stream batch and update the state
    process_stream_task = PythonOperator(
        task_id='process_current_stream_batch',
        python_callable=_process_and_update_state,
        provide_context=True,
    )

    # Define task dependencies: 'get_next_stream_task' must complete before 'process_stream_task' starts.
    get_next_stream_task >> process_stream_task
