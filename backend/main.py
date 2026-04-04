from fastapi import FastAPI, Query
from elasticsearch import Elasticsearch
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List

app = FastAPI(title="TrustShield API", description="AI-Powered Misinformation Detection & Media Verification API")

# 1. Enable CORS for Frontend communication
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, replace with your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

import json
import os

# 2. Connect to Elasticsearch
# Pull from environment variables with defaults
ES_HOST = os.getenv("ES_ENDPOINT", "http://elasticsearch:9200")
INDEX_NAME = os.getenv("ES_INDEX", "trustshield_articles")

es = Elasticsearch(ES_HOST)

@app.get("/")
def read_root():
    return {"status": "TrustShield API is running", "elasticsearch_connected": es.ping()}

@app.get("/health")
def health_check():
    # 1. Check Elasticsearch
    es_status = "Online" if es.ping() else "Offline"

    # 2. Get latest AI Model run info
    latest_run = {}
    history_file = "ml_inference/model_run_history.jsonl"
    
    if os.path.exists(history_file):
        try:
            with open(history_file, "r") as f:
                lines = f.readlines()
                if lines:
                    latest_run = json.loads(lines[-1]) # Get most recent run
        except Exception as e:
            latest_run = {"error": f"Could not read history: {str(e)}"}
    else:
        latest_run = {"status": "No model runs recorded yet"}

    return {
        "system": "Healthy",
        "database": {
            "elasticsearch": es_status,
            "index_name": INDEX_NAME
        },
        "latest_ai_metrics": latest_run
    }

# 3. GET /articles - List recent articles with optional filtering
@app.get("/articles")
def get_articles(
    category: Optional[str] = Query(None, description="Filter by: High Risk, Medium Risk, Low Risk"),
    limit: int = 20
):
    query = {"match_all": {}}
    if category:
        query = {"term": {"credibility_category": category}}

    try:
        response = es.search(
            index=INDEX_NAME,
            body={
                "query": query,
                "size": limit,
                "sort": [{"timestamp": {"order": "desc"}}]
            }
        )
        # Extract just the data (_source) from the ES response
        return [hit["_source"] for hit in response["hits"]["hits"]]
    except Exception as e:
        print(f"Search error: {e}")
        return []

# 4. GET /search - Full-text search endpoint
@app.get("/search")
def search_articles(q: str = Query(..., min_length=1)):
    try:
        response = es.search(
            index=INDEX_NAME,
            body={
                "query": {
                    "multi_match": {
                        "query": q,
                        "fields": ["content_title", "explanation"]
                    }
                },
                "size": 20
            }
        )
        return [hit["_source"] for hit in response["hits"]["hits"]]
    except Exception as e:
        print(f"Search error: {e}")
        return []

# 5. GET /stats - Aggregations for the Dashboard charts
@app.get("/stats")
def get_stats():
    try:
        response = es.search(
            index=INDEX_NAME,
            body={
                "size": 0, # We don't need actual docs, just the counts
                "aggs": {
                    "risk_counts": {
                        "terms": {"field": "credibility_category"}
                    },
                    "source_counts": {
                        "terms": {"field": "source_name"}
                    }
                }
            }
        )
        
        # Simplify the structure for the frontend
        stats = {
            "risk_distribution": {bucket["key"]: bucket["doc_count"] for bucket in response["aggregations"]["risk_counts"]["buckets"]},
            "top_sources": {bucket["key"]: bucket["doc_count"] for bucket in response["aggregations"]["source_counts"]["buckets"]}
        }
        return stats
    except Exception as e:
        print(f"Stats error: {e}")
        return {"risk_distribution": {}, "top_sources": {}}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
