# TrustShield – Power BI over the Gold layer

```
04_ai_inference ──► raw-data/combined/gold/analyzed_data.parquet   (flat Gold, feeds Elasticsearch)
                                   │
06_gold_marts   ──► gold/marts/*.parquet                           (star schema, feeds Power BI)
                                   │  http://localhost:9000  (anonymous read)
                          Power BI Desktop (Import mode)
```

The flat Gold table stays as is. `06_gold_marts` reshapes it into a star schema in a
separate `gold` bucket, so Power BI never touches the `raw-data` bucket.

## Model

| Table | Grain | Key columns |
|---|---|---|
| `fact_articles` | 1 row / article | `article_id`, `source_key`, `date_key`, `misinfo_probability`, `credibility_category`, `entity_count` |
| `fact_entities` | 1 row / entity mention | `article_id`, `entity_type` (PER/ORG/LOC/MISC), `entity_text` |
| `dim_source` | 1 row / source | `source_key`, `source_name` |
| `dim_date` | 1 row / day | `date_key`, `date`, `year`, `month`, `weekday`, `iso_week` |
| `dim_credibility` | 3 rows | `credibility_category`, `sort_order` |

Articles are de-duplicated on a hash of the URL (falls back to source + title).

## Setup

1. Run the DAG once: Airflow UI (http://localhost:8081) → `06_gold_marts` → Trigger.
   Check MinIO (http://localhost:9001) for `gold/marts/*.parquet`. Run it after
   `04_ai_inference` whenever you want the dashboard refreshed.
2. Power BI Desktop → Get Data → Blank Query → Advanced Editor, and paste each block from
   `queries.pq` as its own query. Create `Gold_Base` first and untick *Enable load* on it.
   Choose **Anonymous** when asked for credentials for `http://localhost:9000`.
3. Model view – create these relationships (all many-to-one, single direction):
   - `fact_articles[source_key]` → `dim_source[source_key]`
   - `fact_articles[date_key]` → `dim_date[date_key]`
   - `fact_articles[credibility_category]` → `dim_credibility[credibility_category]`
   - `fact_entities[article_id]` → `fact_articles[article_id]` (many-to-one, filter direction from `fact_articles` to `fact_entities`)
4. Right-click `dim_date` → *Mark as date table* on `date`. Select `dim_credibility[credibility_category]` → *Sort by column* → `sort_order`. Sort `dim_date[month_name]` by `month` and `weekday` by `weekday_number`.
5. Add the measures from `measures.dax`.

## Suggested pages

- **Overview** – cards: `Articles`, `Avg Misinfo Probability`, `High Risk %`; line chart of `Avg Probability 7d Rolling` by `dim_date[date]`; donut of `Articles` by `credibility_category`.
- **Sources** – bar chart of `Source Risk Score` by `source_name` (top N), matrix of source × credibility category, scatter of `Articles` vs `Avg Misinfo Probability`.
- **Entities** – bar chart of `Entity Mentions` by `entity_text` filtered to `entity_type` (slicer), so you can see which people/orgs appear in high-risk articles.
- **Article drill-through** – table of `title`, `source_name`, `misinfo_probability`, `explanation`, `url` (set as web URL); use as a drill-through target from any visual.
- **Model health** – `Model Versions In Data`, average probability by `model_version`, so a model change that shifts scores shows up.

## Caveats

- **Anonymous read.** The `gold/marts/` prefix is publicly readable because Power BI's Web
  connector cannot sign S3 requests. Fine on localhost; don't expose port 9000 or reuse
  this for real data. For a shared setup, put MinIO behind an authenticated proxy, or use
  the Python-script connector with `boto3` and the MinIO credentials instead.
- **Scheduled refresh.** Refresh in Desktop is manual. Refreshing in the Power BI Service
  needs an on-premises data gateway that can reach MinIO, because `localhost` is not
  reachable from the cloud.
- `localhost:9000` is the host-mapped MinIO port from `docker-compose.yml`. If Power BI runs
  on a different machine, change `BaseUrl` in `Gold_Base`.