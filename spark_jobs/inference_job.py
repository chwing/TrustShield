import pandas as pd
import torch
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col
from transformers import pipeline

# 1. Initialize Spark with S3 Config
spark = SparkSession.builder \
    .appName("TrustShield_AI_Inference") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "trustshield_admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "trustshield_password") \
    .getOrCreate()


# 2. Define the Pandas UDF for Distributed Inference
# This function runs on the Spark Workers
@pandas_udf("float")
def predict_credibility_udf(texts: pd.Series) -> pd.Series:
    # Use a specific fake-news detection model
    # 'dhruvpal/fake-news-bert' is a popular free choice on HuggingFace
    classifier = pipeline(
        "text-classification",
        model="dhruvpal/fake-news-bert",
        device=-1  # Set to 0 if you have a GPU
    )

    results = classifier(texts.to_list(), truncation=True, max_length=512)
    # The model returns 'LABEL_1' (Real) or 'LABEL_0' (Fake)
    # We convert this to a 0-100 score
    scores = [res['score'] * 100 if res['label'] == 'LABEL_1' else (1 - res['score']) * 100 for res in results]
    return pd.Series(scores)


# 3. Load Silver Data & Run Inference
df = spark.read.parquet("s3a://raw-data/combined/processed/cleaned_data.parquet")

# Filter only new data that hasn't been scored yet (optional logic)
# We score 'content_title' which is our unified column
scored_df = df.withColumn("credibility_score", predict_credibility_udf(col("content_title")))

# 4. Save to Gold Layer
scored_df.write.mode("overwrite").parquet("s3a://raw-data/combined/gold/final_scores_spark.parquet")

spark.stop()