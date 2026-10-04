import hashlib
import io
import json

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from index_to_es import read_s3_parquet

SOURCE_BUCKET = 'raw-data'
GOLD_INPUT = 'combined/gold/analyzed_data.parquet'
MART_BUCKET = 'gold'
MART_PREFIX = 'marts'

# Power BI's Web connector cannot sign S3 requests, so the marts bucket is
# anonymously readable. Fine for local dev; do NOT expose port 9000 publicly.
PUBLIC_READ_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": ["*"]},
        "Action": ["s3:GetObject"],
        "Resource": [f"arn:aws:s3:::{MART_BUCKET}/{MART_PREFIX}/*"],
    }],
}

CREDIBILITY_ORDER = [
    ("Low Risk", 1, 0.0, 0.3),
    ("Medium Risk", 2, 0.3, 0.6),
    ("High Risk", 3, 0.6, 1.0),
]


def _article_id(row):
    key = row['url'] if isinstance(row['url'], str) and row['url'] else f"{row['source_name']}|{row['content_title']}"
    return hashlib.sha1(key.encode('utf-8')).hexdigest()[:16]


def _parse_entities(raw):
    try:
        items = json.loads(raw)
    except (TypeError, ValueError):
        return []
    return [(e.get('entity'), str(e.get('word', '')).replace('##', '').strip()) for e in items]


def build_marts(gold):
    """Turn the flat gold table into a star schema. Returns {table_name: DataFrame}."""
    df = gold.copy()
    df['published_at'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df['article_id'] = df.apply(_article_id, axis=1)
    # Re-runs of inference can produce the same article twice; keep the latest.
    df = df.sort_values('published_at').drop_duplicates('article_id', keep='last')

    dim_source = (
        pd.DataFrame({'source_name': sorted(df['source_name'].dropna().unique())})
        .reset_index(names='source_key')
    )
    dim_source['source_key'] += 1

    dated = df['published_at'].dropna()
    if dated.empty:
        dim_date = pd.DataFrame(columns=['date_key', 'date', 'year', 'month', 'month_name',
                                         'day', 'weekday', 'weekday_number', 'iso_week'])
    else:
        days = pd.date_range(dated.min().normalize(), dated.max().normalize(), freq='D')
        dim_date = pd.DataFrame({
            'date_key': days.strftime('%Y%m%d').astype(int),
            'date': days.date,
            'year': days.year,
            'month': days.month,
            'month_name': days.strftime('%b'),
            'day': days.day,
            'weekday': days.strftime('%a'),
            'weekday_number': days.dayofweek + 1,
            'iso_week': days.isocalendar().week.astype(int).values,
        })

    dim_credibility = pd.DataFrame(
        CREDIBILITY_ORDER,
        columns=['credibility_category', 'sort_order', 'min_probability', 'max_probability'],
    )

    entity_rows = [
        (aid, etype, etext)
        for aid, raw in zip(df['article_id'], df['entities'])
        for etype, etext in _parse_entities(raw)
        if etext
    ]
    fact_entities = pd.DataFrame(entity_rows, columns=['article_id', 'entity_type', 'entity_text'])
    entity_counts = fact_entities.groupby('article_id').size()

    fact = df.merge(dim_source, on='source_name', how='left')
    fact['source_key'] = fact['source_key'].astype('Int64')
    fact['date_key'] = fact['published_at'].dt.strftime('%Y%m%d').astype('Int64') \
        if fact['published_at'].notna().any() else pd.NA
    fact['entity_count'] = fact['article_id'].map(entity_counts).fillna(0).astype(int)
    fact_articles = fact[[
        'article_id', 'source_key', 'date_key', 'published_at', 'content_title', 'url',
        'misinfo_probability', 'credibility_category', 'explanation', 'entity_count',
        'model_version', 'model_engine',
    ]].rename(columns={'content_title': 'title'})

    return {
        'fact_articles': fact_articles,
        'fact_entities': fact_entities,
        'dim_source': dim_source,
        'dim_date': dim_date,
        'dim_credibility': dim_credibility,
    }


def _ensure_bucket(s3):
    if not s3.check_for_bucket(MART_BUCKET):
        s3.create_bucket(bucket_name=MART_BUCKET)
    s3.get_conn().put_bucket_policy(Bucket=MART_BUCKET, Policy=json.dumps(PUBLIC_READ_POLICY))


def build_gold_marts():
    s3 = S3Hook(aws_conn_id='minio_conn')

    print(f"Reading gold data from {GOLD_INPUT}...")
    gold = read_s3_parquet(s3, SOURCE_BUCKET, GOLD_INPUT)
    if gold.empty:
        print("No gold data to model.")
        return

    marts = build_marts(gold)
    _ensure_bucket(s3)

    for name, table in marts.items():
        buf = io.BytesIO()
        # Power BI's Parquet reader is unreliable with ns timestamps.
        table.to_parquet(buf, index=False, coerce_timestamps='ms', allow_truncated_timestamps=True)
        buf.seek(0)
        key = f"{MART_PREFIX}/{name}.parquet"
        s3.load_file_obj(file_obj=buf, key=key, bucket_name=MART_BUCKET, replace=True)
        print(f"Wrote {len(table)} rows to s3://{MART_BUCKET}/{key}")


if __name__ == "__main__":
    build_gold_marts()
