from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def transform_news_with_spark():
    # 1. Create Spark Session with MinIO Config
    spark = None
    try:
        # Define paths to pre-downloaded JARs
        hadoop_aws_jar = "/home/airflow/.ivy2/jars/hadoop-aws-3.3.4.jar"
        aws_sdk_jar = "/home/airflow/.ivy2/jars/aws-java-sdk-bundle-1.12.262.jar"
        
        spark = SparkSession.builder \
            .appName("TrustShield_Transformation") \
            .master("spark://spark-master:7077") \
            .config("spark.jars", f"{hadoop_aws_jar},{aws_sdk_jar}") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "trustshield_admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "trustshield_password") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.network.timeout", "600s") \
            .getOrCreate()

        # 2. Read the Raw JSON from MinIO
        # We use a wildcard (*) to pick up any raw files from all sources
        print("Reading raw data from MinIO...")
        raw_df = spark.read.json("s3a://raw-data/*/raw/*.json")

        if raw_df.rdd.isEmpty():
            print("No raw data found to process.")
            return

        # 3. Transform: Unified schema for different sources
        # We explode 'articles' which is the common wrapper we now use
        base_df = raw_df.select(F.explode("articles").alias("item"))

        # Map fields dynamically based on what's available
        clean_df = base_df.select(
            F.coalesce(F.col("item.source.name"), F.lit("Bluesky/RSS")).alias("source_name"),
            F.coalesce(F.col("item.author"), F.lit("Unknown")).alias("author"),
            F.coalesce(F.col("item.title"), F.col("item.text")).alias("content_title"),
            F.coalesce(F.col("item.description"), F.col("item.summary"), F.lit("")).alias("summary"),
            F.coalesce(F.col("item.url"), F.col("item.link"), F.col("item.uri")).alias("url"),
            F.coalesce(F.col("item.publishedAt"), F.col("item.published"), F.col("item.created_at")).alias("timestamp")
        )

        # 4. Save as Parquet (The "Silver" Layer)
        output_path = "s3a://raw-data/combined/processed/cleaned_data.parquet"
        print(f"Saving cleaned data to {output_path}...")
        clean_df.write.mode("overwrite").parquet(output_path)

        print(f"Spark transformation complete! Processed data into /combined/processed/ folder.")
    except Exception as e:
        print(f"Error during Spark transformation: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped.")


with DAG(
        dag_id='03_spark_transformation',
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False
) as dag:
    spark_task = PythonOperator(
        task_id='clean_news_data',
        python_callable=transform_news_with_spark
    )