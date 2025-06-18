from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'analytics',
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_edtech_analytics',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    catchup=False
) as dag:

    extract = BashOperator(
        task_id='extract_events',
        bash_command='python /opt/scripts/extract_from_s3.py {{ ds }}'
    )

    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='cd /opt/dbt && dbt run --models +* --vars "{\"run_date\":\"{{ ds }}\"}"'
    )

    extract >> run_dbt