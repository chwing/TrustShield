import json
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# --- CONFIGURATION ---
NEWS_API_KEY = 'ea314001e87b49fcbc856140936a5507'
QUERY = 'misinformation OR "fake news" OR AI'  # Topics for TrustShield
BUCKET_NAME = 'raw-data'


def fetch_and_land_news():
    # 1. Fetch from NewsAPI
    url = f'https://newsapi.org/v2/everything?q={QUERY}&apiKey={NEWS_API_KEY}&pageSize=20'
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"API Error: {response.text}")

    data = response.json()

    # 2. Add metadata (standard data engineering practice)
    data['ingestion_metadata'] = {
        'ingested_at': datetime.now().isoformat(),
        'source_system': 'NewsAPI',
        'pipeline': 'TrustShield_Ingestion_V1'
    }

    # 3. Create a unique filename based on time
    execution_date = datetime.now().strftime("%Y-%m-%d_%H-%M")
    file_path = f"news/raw/{execution_date}_news.json"

    # 4. Upload using our verified minio_conn
    s3 = S3Hook(aws_conn_id='minio_conn')
    if not s3.check_for_bucket(BUCKET_NAME):
        s3.create_bucket(BUCKET_NAME)
        print(f"Created bucket: {BUCKET_NAME}")

    s3.load_string(
        string_data=json.dumps(data, default=str),
        key=file_path,
        bucket_name=BUCKET_NAME,
        replace=True
    )
    print(f"Successfully landed {len(data['articles'])} articles to {file_path}")


with DAG(
        dag_id='02_news_ingestion',
        start_date=datetime(2024, 1, 1),
        schedule_interval='@daily',
        catchup=False,
        tags=['ingestion', 'news']
) as dag:
    ingest_task = PythonOperator(
        task_id='fetch_news_to_minio',
        python_callable=fetch_and_land_news
    )