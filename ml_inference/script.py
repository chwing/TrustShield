from transformers import pipeline
import pandas as pd
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import io
import sys
import os

# Add paths for local imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'airflow/plugins'))

from tracker import TrustShieldModelTracker, get_model_metadata
try:
    from data_quality import run_quality_gate
except ImportError:
    # If called outside Airflow/Plugins context
    def run_quality_gate(df): return True

def read_s3_parquet(s3, bucket, prefix):
    """Helper to read multi-part parquet from S3/MinIO."""
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

def run_ai_inference():
    # 0. Initialize Model Tracker & Env Vars
    model_name = os.getenv("DEFAULT_CLASSIFIER", "facebook/bart-large-mnli")
    ner_model = os.getenv("DEFAULT_NER", "dbmdz/bert-large-cased-finetuned-conll03-english")
    
    tracker = TrustShieldModelTracker(model_name, "Misinformation Detection")
    tracker.start_run()

    # 1. Load the AI Models
    print(f"Loading AI model: {model_name}...")
    classifier = pipeline("zero-shot-classification",
                          model=model_name,
                          device=-1)  # -1 uses CPU

    print(f"Loading NER model: {ner_model}...")
    ner_pipe = pipeline("token-classification", 
                         model=ner_model, 
                         aggregation_strategy="simple",
                         device=-1)

    # 2. Connect to MinIO and load data
    s3 = S3Hook(aws_conn_id='minio_conn')
    bucket_name = 'raw-data'
    input_path = 'combined/processed/cleaned_data.parquet'
    
    print(f"Reading silver data from {input_path}...")
    
    # Download parquet from S3
    try:
        df = read_s3_parquet(s3, bucket_name, input_path)
    except Exception as e:
        print(f"Error reading silver data: {e}")
        return
    
    if df.empty:
        print("No data found to analyze.")
        return

    candidate_labels = ["reliable", "misinformation", "satire"]
    reason_labels = ["emotional language", "clickbait", "lack of citations", "objective reporting"]

    def analyze_text(text):
        if not text or str(text).strip() == "": 
            return 0.5, "No content to analyze.", "[]"
        try:
            # 1. Get Misinformation Score
            result = classifier(str(text), candidate_labels)
            idx = result['labels'].index('misinformation')
            prob = float(result['scores'][idx])

            # 2. Get Red Flags for Explanation
            reason_result = classifier(str(text), reason_labels)
            
            # Find top red flags (excluding objective reporting)
            red_flags = [
                label for label, score in zip(reason_result['labels'], reason_result['scores'])
                if label != "objective reporting" and score > 0.3
            ]
            
            if prob < 0.3:
                reason = "The content appears to be objective and reliable."
            else:
                if red_flags:
                    reason = f"Possible issues detected: {', '.join(red_flags)}."
                else:
                    reason = "The content shows patterns common in misinformation, though no specific red flags were dominant."
            
            # 3. Get Entities (NER)
            ner_results = ner_pipe(str(text))
            entities = [{"entity": e['entity_group'], "word": e['word']} for e in ner_results]
            
            return prob, reason, json.dumps(entities)
        except Exception as e:
            print(f"Error analyzing text: {e}")
            return 0.5, "Analysis failed.", "[]"

    # 3. Process Data
    print(f"AI is analyzing {len(df)} headlines...")
    results = df['content_title'].apply(analyze_text)
    df['misinfo_probability'] = [r[0] for r in results]
    df['explanation'] = [r[1] for r in results]
    df['entities'] = [r[2] for r in results]

    # 4. Final Structured Transformation (Gold Layer Prep)
    print("Structuring final dataset for Gold Layer...")
    
    def get_category(prob):
        if prob < 0.3: return "Low Risk"
        if prob < 0.6: return "Medium Risk"
        return "High Risk"

    df['credibility_category'] = df['misinfo_probability'].apply(get_category)
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

    # Add Model Metadata for Version Tracking
    metadata = get_model_metadata(model_name)
    df['model_version'] = metadata['model_version']
    df['model_engine'] = metadata['model_engine']

    final_columns = [
        'source_name', 'content_title', 'url', 'timestamp', 
        'misinfo_probability', 'credibility_category', 
        'explanation', 'entities', 'model_version', 'model_engine'
    ]
    
    available_columns = [c for c in final_columns if c in df.columns]
    gold_df = df[available_columns].copy()

    # 5. Data Quality Gate (Stop-the-Line Check)
    run_quality_gate(gold_df)

    # 6. End Tracker Run
    tracker.log_metrics(gold_df)
    tracker.end_run()

    # 7. Save to the 'Gold' Layer
    output_path = 'combined/gold/analyzed_data.parquet'
    out_buffer = io.BytesIO()
    gold_df.to_parquet(out_buffer, index=False)
    out_buffer.seek(0)
    
    s3.load_file_obj(
        file_obj=out_buffer,
        key=output_path,
        bucket_name=bucket_name,
        replace=True
    )
    
    print(f"AI Analysis complete. Data promoted to GOLD layer at {output_path}.")
