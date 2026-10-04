from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# Ensure ml_inference is in the path
sys.path.append('/opt/airflow/ml_inference')
from build_gold_marts import build_gold_marts

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
    '06_gold_marts',
    default_args=default_args,
    description='Model Gold layer into star-schema Parquet marts for Power BI',
    schedule_interval=None,
    catchup=False,
    tags=['powerbi', 'marts', 'gold'],
) as dag:

    marts_task = PythonOperator(
        task_id='build_star_schema',
        python_callable=build_gold_marts,
    )