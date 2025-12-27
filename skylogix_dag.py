from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/mnt/c/Users/Ridwan/Skylogix_pipeline')

from ingest_data import fetch_and_upsert_raw
from transform_data import transform_and_load


default_args = {
    'owner': 'Skylogix_Admin',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'skylogix_weather_pipeline',
    default_args=default_args,
    description='Skylogix: Ingest from API to Mongo, then Transform to Postgres',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    # Task 1: Ingest into MongoDB
    ingest_task = BashOperator(
        task_id='ingest_weather_data',
        bash_command='python3 /mnt/c/Users/Ridwan/Skylogix_pipeline/ingest_data.py'
    )

    # Task 2: Transform into PostgreSQL
    transform_task = BashOperator(
        task_id='transform_to_postgres',
        bash_command='python3 /mnt/c/Users/Ridwan/Skylogix_pipeline/transform_data.py'
    )

    # Dependency: Ingest must finish before Transform starts
    ingest_task >> transform_task
