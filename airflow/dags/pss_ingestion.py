from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import feedparser
from datetime import datetime
import json

rss_raw_data = Dataset("minio://raw/rss")


def fetch_rss_feed():
    feed_url = "http://feeds.bbci.co.uk/news/world/rss.xml"
    feed = feedparser.parse(feed_url)

    articles = []
    for entry in feed.entries:
        articles.append({
            "title": entry.title,
            "link": entry.link,
            "published": entry.published,
            "summary": entry.get("summary", "")
        })

    # Logic to upload to MinIO raw bucket
    if articles:
        execution_date = datetime.now().strftime("%Y-%m-%d_%H-%M")
        file_path = f"rss/raw/{execution_date}_rss.json"
        bucket_name = 'raw-data'
        
        s3 = S3Hook(aws_conn_id='minio_conn')
        if not s3.check_for_bucket(bucket_name):
            s3.create_bucket(bucket_name)
            print(f"Created bucket: {bucket_name}")

        s3.load_string(
            string_data=json.dumps({"articles": articles}, default=str),
            key=file_path,
            bucket_name=bucket_name,
            replace=True
        )
        print(f"Ingested {len(articles)} articles from BBC RSS and uploaded to {file_path}")
    else:
        print("No articles found in BBC RSS feed.")


with DAG(
        "ingestion_rss",
        start_date=datetime(2024, 1, 1),
        schedule="*/30 * * * *",  # Every 30 minutes
        catchup=False
) as dag:
    fetch_rss = PythonOperator(
        task_id="fetch_world_news_rss",
        python_callable=fetch_rss_feed,
        outlets=[rss_raw_data]
    )