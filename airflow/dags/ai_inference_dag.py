from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add ml_inference to path so we can import the script
sys.path.append('/opt/airflow/ml_inference')
from script import run_ai_inference

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
    '04_ai_inference',
    default_args=default_args,
    description='Run AI inference on cleaned data',
    schedule_interval=None,
    catchup=False,
    tags=['ai', 'inference', 'gold'],
) as dag:

    inference_task = PythonOperator(
        task_id='run_misinformation_detection',
        python_callable=run_ai_inference,
    )
