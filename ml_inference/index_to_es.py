from elasticsearch import Elasticsearch, helpers
import pandas as pd
import io
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def read_s3_parquet(s3, bucket, prefix):
    """Helper to read parquet from S3/MinIO (handles both file and folder)."""
    # Try to see if it's a single file first
    if s3.check_for_key(prefix, bucket):
        print(f"Reading single parquet file: {prefix}")
        key_obj = s3.get_key(prefix, bucket)
        data = io.BytesIO(key_obj.get()['Body'].read())
        return pd.read_parquet(data)
    
    # If not a single file, treat as a directory
    keys = s3.list_keys(bucket, prefix=prefix)
    if not keys:
        return pd.DataFrame()
    
    # Filter for actual data parts
    part_keys = [k for k in keys if k.endswith('.parquet')]
    if not part_keys:
        return pd.DataFrame()
    
    dfs = []
    for key in part_keys:
        print(f"Downloading part: {key}")
        key_obj = s3.get_key(key, bucket)
        data = io.BytesIO(key_obj.get()['Body'].read())
        dfs.append(pd.read_parquet(data))
    
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def index_gold_data_to_es():
    # 1. Connect to Elasticsearch & MinIO
    es = Elasticsearch("http://elasticsearch:9200")
    s3 = S3Hook(aws_conn_id='minio_conn')
    bucket_name = 'raw-data'
    input_path = 'combined/gold/analyzed_data.parquet'
    index_name = "trustshield_articles"

    print(f"Reading gold data from {input_path}...")
    try:
        df = read_s3_parquet(s3, bucket_name, input_path)
    except Exception as e:
        print(f"Error reading gold data: {e}")
        return

    if df.empty:
        print("No gold data to index.")
        return

    # 2. Define Index Mapping (Schema)
    # This tells ES how to treat each field
    mapping = {
        "mappings": {
            "properties": {
                "source_name": {"type": "keyword"},
                "content_title": {"type": "text", "analyzer": "english"},
                "url": {"type": "keyword"},
                "timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                "misinfo_probability": {"type": "float"},
                "credibility_category": {"type": "keyword"},
                "explanation": {"type": "text"},
                "entities": {"type": "nested"}  # Allows searching inside the entities list
            }
        }
    }

    # Re-create index if it doesn't exist
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=mapping)
        print(f"Created index: {index_name}")

    # 3. Prepare data for Bulk Upload
    def generate_actions():
        for i, row in df.iterrows():
            # Parse entities back to JSON object for nested indexing
            try:
                entities_list = json.loads(row['entities'])
            except:
                entities_list = []

            yield {
                "_index": index_name,
                "_id": f"{row['source_name']}_{i}", # Unique ID for each doc
                "_source": {
                    "source_name": row['source_name'],
                    "content_title": row['content_title'],
                    "url": row['url'],
                    "timestamp": row['timestamp'],
                    "misinfo_probability": row['misinfo_probability'],
                    "credibility_category": row['credibility_category'],
                    "explanation": row['explanation'],
                    "entities": entities_list
                }
            }

    # 4. Perform Bulk Indexing
    print(f"Indexing {len(df)} articles into Elasticsearch...")
    success, failed = helpers.bulk(es, generate_actions())
    
    print(f"Successfully indexed {success} documents. Failed: {failed}")

if __name__ == "__main__":
    index_gold_data_to_es()
