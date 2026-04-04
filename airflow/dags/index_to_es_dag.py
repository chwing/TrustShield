from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Ensure ml_inference is in the path
sys.path.append('/opt/airflow/ml_inference')
from index_to_es import index_gold_data_to_es

default_args = {
    'owner': 'trustshield',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '05_index_to_es',
    default_args=default_args,
    description='Index Gold layer data into Elasticsearch',
    schedule_interval=None,
    catchup=False,
    tags=['elasticsearch', 'index', 'gold'],
) as dag:

    index_task = PythonOperator(
        task_id='bulk_index_to_elasticsearch',
        python_callable=index_gold_data_to_es,
    )
