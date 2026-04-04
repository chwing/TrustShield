from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from atproto import Client
from datetime import datetime
import json

bluesky_raw_data = Dataset("minio://raw/bluesky")


def fetch_bluesky_trending():
    client = Client()
    # Use your handle and the App Password you generated
    client.login('chwingss.bsky.social', 'z2f7-hhyk-lbrv-wivq')

    # Searching for specific high-risk keywords for misinformation
    # Or you can fetch your timeline/popular feeds
    params = {'q': 'breaking news', 'limit': 50}
    results = client.app.bsky.feed.search_posts(params=params)

    posts = []
    for post in results.posts:
        posts.append({
            "text": post.record.text,
            "author": post.author.handle,
            "created_at": post.record.created_at,
            "uri": post.uri
        })

    # Logic to upload to MinIO
    if posts:
        execution_date = datetime.now().strftime("%Y-%m-%d_%H-%M")
        file_path = f"bluesky/raw/{execution_date}_bluesky.json"
        bucket_name = 'raw-data'
        
        s3 = S3Hook(aws_conn_id='minio_conn')
        if not s3.check_for_bucket(bucket_name):
            s3.create_bucket(bucket_name)
            print(f"Created bucket: {bucket_name}")

        s3.load_string(
            string_data=json.dumps({"articles": posts}, default=str),
            key=file_path,
            bucket_name=bucket_name,
            replace=True
        )
        print(f"Captured {len(posts)} Bluesky posts and uploaded to {file_path}.")
    else:
        print("No Bluesky posts captured.")


with DAG(
        "ingestion_bluesky",
        start_date=datetime(2024, 1, 1),
        schedule="@hourly",
        catchup=False
) as dag:
    fetch_task = PythonOperator(
        task_id = "fetch_bluesky_posts",
        python_callable = fetch_bluesky_trending,
        outlets = [bluesky_raw_data]
    )
