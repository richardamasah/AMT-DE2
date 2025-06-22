from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'sir_djanie',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    dag_id='music_kpi_pipeline',
    default_args=default_args,
    description='ETL + KPI + Dynamo pipeline',
    schedule_interval=None,  # or '0 6 * * *' for daily 6 AM UTC
    start_date=datetime(2024, 6, 21),
    catchup=False,
    tags=['music', 'kpi', 'dynamo'],
) as dag:

    Ingestion_n_validation = GlueJobOperator(
        task_id='run_glue_etl_transform',
        job_name='Data-validation-job',
        script_location='s3://your-bucket/scripts/job1_etl.py',  # optional if Glue handles it
        aws_conn_id='aws_default',
        region_name='us-east-1',
        dag=dag
    )

    kpi_computation = GlueJobOperator(
        task_id='run_glue_kpi_compute',
        job_name='kpi_computation_job',
        aws_conn_id='aws_default',
        region_name='us-east-1',
        dag=dag
    )

    load_dynamo = GlueJobOperator(
        task_id='run_kpi_to_dynamo',
        job_name='load-to-dynamo',
        aws_conn_id='aws_default',
        region_name='us-east-1',
        dag=dag
    )

    Ingestion_n_validation >> kpi_computation >> load_dynamo
