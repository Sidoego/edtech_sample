from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import boto3
import os

S3_BUCKET = 'your-s3-bucket-name'
S3_KEY = 'path/to/raw_events.jsonl'
LOCAL_PATH = '/tmp/raw_events.jsonl'

default_args = {
    'owner': 'analytics',
    'start_date': datetime(2025, 6, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def download_from_s3():
    s3 = boto3.client('s3')
    os.makedirs(os.path.dirname(LOCAL_PATH), exist_ok=True)
    s3.download_file(S3_BUCKET, S3_KEY, LOCAL_PATH)
    print(f"Downloaded s3://{S3_BUCKET}/{S3_KEY} to {LOCAL_PATH}")

with DAG(
    dag_id='load_raw_data',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    max_active_runs=1
) as dag_load:

    wait_for_file = S3KeySensor(
        task_id='wait_for_raw_events_file',
        bucket_name=S3_BUCKET,
        bucket_key=S3_KEY,
        aws_conn_id='aws_default',
        poke_interval=60,
        timeout=60*60*3,
        mode='reschedule'
    )

    download_file = PythonOperator(
        task_id='download_raw_events_file',
        python_callable=download_from_s3
    )

    load_to_ch = BashOperator(
        task_id='load_to_clickhouse',
        bash_command='python3 /path/to/parse_raw_events_with_ch.py',
        env={
            'INPUT_FILE': LOCAL_PATH,
            'CLICKHOUSE_HOST': 'clickhouse-host.example.com',
            'CLICKHOUSE_PORT': '8123',
            'CLICKHOUSE_USER': 'default',
            'CLICKHOUSE_PASSWORD': 'password',
            'CLICKHOUSE_DB': 'edtech_analytics',
        }
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_analytics",
        trigger_dag_id="run_dbt_analytics",
        wait_for_completion=False,
        reset_dag_run=True,
        poke_interval=30
    )

    wait_for_file >> download_file >> load_to_ch >> trigger_dbt
