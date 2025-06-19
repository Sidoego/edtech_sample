from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'analytics',
    'start_date': datetime(2025, 6, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='run_dbt_analytics',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1
) as dag_dbt:

    run_dbt = BashOperator(
        task_id='dbt_run',
        bash_command='cd /path/to/edtech_analytics_dbt && dbt run --profiles-dir /path/to/profiles'
    )
