# TrustShield: AI-Powered Misinformation Detection Pipeline

TrustShield is a production-oriented data engineering and NLP system designed to detect and explain misinformation in real-time. It processes data through a modern Data Lake architecture (Bronze, Silver, Gold) and uses state-of-the-art Hugging Face models for analysis.

## 🚀 Key Features
- **Automated Ingestion:** Airflow DAGs for NewsAPI, RSS, and social media feeds.
- **Data Lake Architecture:** Structured processing from raw JSON to cleaned Parquet (Silver) and analyzed Parquet (Gold).
- **AI Inference:** 
  - **Zero-Shot Classification:** Detects misinformation and provides reasoning (e.g., clickbait, emotional language).
  - **NER:** Extracts People, Organizations, and Locations for searchable metadata.
- **Search & Analytics:** Indexed in **Elasticsearch** for millisecond-latency search and aggregations.
- **Professional UI:** React (Vite) dashboard with real-time risk charts and article feeds.
- **Production Guardrails:** Custom Data Quality checks and Model Versioning/Tracking.

## 🛠️ Tech Stack
- **Infrastructure:** Docker, Docker Compose, MinIO (S3 compatible)
- **Data Engineering:** Apache Airflow, Apache Spark (PySpark)
- **AI/ML:** Hugging Face Transformers (BART, BERT)
- **Backend:** FastAPI, Elasticsearch
- **Frontend:** React, Tailwind CSS, Recharts

## 🏗️ Architecture
1. **Ingestion:** Airflow pulls raw data into the **Bronze Layer** (MinIO).
2. **Processing:** Spark cleans and unifies data into the **Silver Layer**.
3. **Inference:** Hugging Face models analyze text, generating scores, reasons, and entities.
4. **Validation:** A Data Quality Gate ensures the data meets production standards before promotion to the **Gold Layer**.
5. **Serving:** The Gold data is indexed in **Elasticsearch** and served via **FastAPI** to the **React Dashboard**.

## 🚦 Getting Started
1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/trustshield.git
   ```
2. **Spin up the stack:**
   ```bash
   docker-compose up -d --build
   ```
3. **Access the tools:**
   - **Dashboard:** `http://localhost:3000`
   - **API Docs:** `http://localhost:8000/docs`
   - **Airflow:** `http://localhost:8081`
   - **MinIO:** `http://localhost:9001`

## 📊 Monitoring
Check the system health and latest AI metrics at:
`http://localhost:8000/health`

---
*Built for the next generation of trustworthy media verification.*
