# Airflow

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_data():
    source_s3_bucket = 'your-source-bucket'
    source_s3_key = 'path/to/source/file'

    s3_hook = S3Hook(aws_conn_id='aws_default')

    # Read the data from the source bucket
    return s3_hook.read_key(key=source_s3_key, bucket_name=source_s3_bucket)

def push_data(**kwargs):
    destination_s3_bucket = 'your-destination-bucket'
    destination_s3_key = 'path/to/destination/file'

    s3_hook = S3Hook(aws_conn_id='aws_default')

    # Retrieve the data passed from the previous task
    data = kwargs['ti'].xcom_pull(task_ids='extract_data')

    # Write the data to the destination bucket
    s3_hook.load_string(data, key=destination_s3_key, bucket_name=destination_s3_bucket)

dag = DAG('s3_to_s3_transfer', default_args=default_args, schedule_interval=None)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

push_task = PythonOperator(
    task_id='push_data',
    python_callable=push_data,
    provide_context=True,
    dag=dag
)

extract_task >> push_task
