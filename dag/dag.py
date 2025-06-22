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

    glue_etl = GlueJobOperator(
        task_id='run_glue_etl_transform',
        job_name='job_etl_transform',
        script_location='s3://your-bucket/scripts/job1_etl.py',  # optional if Glue handles it
        aws_conn_id='aws_default',
        region_name='us-east-1',
        dag=dag
    )

    glue_kpi = GlueJobOperator(
        task_id='run_glue_kpi_compute',
        job_name='job_kpi_compute',
        aws_conn_id='aws_default',
        region_name='us-east-1',
        dag=dag
    )

    glue_load_dynamo = GlueJobOperator(
        task_id='run_kpi_to_dynamo',
        job_name='job_load_to_dynamo',
        aws_conn_id='aws_default',
        region_name='us-east-1',
        dag=dag
    )

    glue_etl >> glue_kpi >> glue_load_dynamo
