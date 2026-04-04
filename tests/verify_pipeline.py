import requests
import json
import time

def verify_system():
    print("--- TrustShield System Verification ---")
    
    # 1. Check Backend Health
    try:
        res = requests.get("http://localhost:8000/health")
        if res.status_code == 200:
            print("✅ Backend: Online")
            print(f"   - ES Connected: {res.json()['database']['elasticsearch']}")
            print(f"   - Latest AI Metric: {res.json()['latest_ai_metrics'].get('timestamp', 'No runs yet')}")
        else:
            print(f"❌ Backend: Status {res.status_code}")
    except Exception as e:
        print(f"❌ Backend: Offline ({e})")

    # 2. Check Elasticsearch Index
    try:
        res = requests.get("http://localhost:9200/trustshield_articles/_count")
        if res.status_code == 200:
            count = res.json()['count']
            print(f"✅ Elasticsearch: Found {count} indexed articles")
        else:
            print(f"❌ Elasticsearch: Index not found or error {res.status_code}")
    except Exception as e:
        print(f"❌ Elasticsearch: Offline ({e})")

    # 3. Check Data Quality Stats
    try:
        res = requests.get("http://localhost:8000/stats")
        if res.status_code == 200:
            stats = res.json()
            print("✅ Stats API: Returning data")
            print(f"   - Risk Distribution: {stats['risk_distribution']}")
        else:
            print(f"❌ Stats API: Error {res.status_code}")
    except Exception as e:
        print(f"❌ Stats API: Failed ({e})")

if __name__ == "__main__":
    verify_system()
