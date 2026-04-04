# TrustShield: System Operations Guide

TrustShield is an automated AI-driven pipeline designed to ingest, transform, and analyze news data to detect misinformation in real-time.

## How the Pipeline Functions Automatically
The system follows a "Medallion Architecture" (Bronze → Silver → Gold) managed by Apache Airflow:

1.  **Ingestion (Bronze Layer):** Airflow DAGs (`02_news_ingestion`, `ingestion_bluesky`, `ingestion_rss`) periodically fetch raw data from external APIs and store it as JSON in **MinIO**.
2.  **Transformation (Silver Layer):** The `03_spark_transformation` DAG uses **Apache Spark** to clean, deduplicate, and unify the various JSON schemas into a single optimized **Parquet** format.
3.  **AI Inference (Gold Layer):** The `04_ai_inference` DAG runs **Hugging Face Transformers** (BART and BERT) over the Silver data to calculate misinformation probability scores, extract entities (NER), and generate human-readable explanations.
4.  **Indexing & Serving:** The finalized "Gold" data is indexed into **Elasticsearch** via the `05_index_to_es` DAG and served through a **FastAPI** backend to the **React Dashboard**.

---

## Access Points (Localhost)

| Service | URL | Credentials |
| :--- | :--- | :--- |
| **Frontend Dashboard** | [http://localhost:3000](http://localhost:3000) | N/A |
| **Airflow Webserver** | [http://localhost:8081](http://localhost:8081) | `admin` / `admin` |
| **Backend API Docs** | [http://localhost:8000/docs](http://localhost:8000/docs) | N/A |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `trustshield_admin` / `trustshield_password` |
| **Spark Master UI** | [http://localhost:8082](http://localhost:8082) | N/A |
| **Elasticsearch API** | [http://localhost:9200](http://localhost:9200) | N/A |

---

## Steps to Test the Project

To verify the system is working from end-to-end, follow these manual trigger steps in Airflow:

### Step 1: Ingest Raw Data
Log into Airflow and trigger one of the ingestion DAGs (e.g., `02_news_ingestion`). 
*   **Verification:** Check MinIO at `http://localhost:9001` in the `raw-data` bucket. You should see new JSON files in the `news/raw/` folder.

### Step 2: Run Spark Transformation
Trigger the `03_spark_transformation` DAG. This will consolidate all raw JSONs.
*   **Verification:** Check MinIO for a new folder: `combined/processed/cleaned_data.parquet`.

### Step 3: Run AI Analysis
Trigger the `04_ai_inference` DAG. This runs the NLP models.
*   **Verification:** Check MinIO for the Gold data: `combined/gold/analyzed_data.parquet`.
*   **Note:** This step can take several minutes depending on the volume of data, as it runs deep learning models on the CPU.

### Step 4: Index to Elasticsearch
Trigger the `05_index_to_es` DAG to push the analyzed data into the search engine.
*   **Verification:** Visit `http://localhost:8000/articles` to see if the API returns the analyzed results.

### Step 5: View the Dashboard
Open the Frontend at `http://localhost:3000`. You should see the risk charts and the feed populated with the articles you just processed.

---

## Technical Notes

### Parquet Multi-part Handling
Data transformed by Spark is stored as a multi-part directory (`.parquet/`). The AI Inference and Indexing scripts have been updated with a `read_s3_parquet` helper to automatically discover and merge these part files.

### Backend Resilience
The backend API has been updated to handle cases where the Elasticsearch index has not yet been created. Instead of a 500 error, it will now return empty results, allowing the Frontend to load gracefully even before the first full pipeline run.

### Spark Versioning
Ensure Spark Worker and Master are running version `3.4.1` to match the Airflow driver. This is pre-configured in the `Dockerfile.spark`.
