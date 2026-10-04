// TrustShield – Power Query (M) queries for the Gold star schema in MinIO.
//
// In Power BI Desktop: Home > Get Data > Blank Query > Advanced Editor.
// Create ONE query per block below, named exactly as the `// name:` header.
// When prompted for credentials on http://localhost:9000, choose "Anonymous".
//
// Every table is a Parquet file at  http://localhost:9000/gold/marts/<table>.parquet
// written by the Airflow DAG `06_gold_marts`.

// ---------------------------------------------------------------- name: Gold_Base
// Shared helper. Other queries call it, so the MinIO address lives in one place.
// (Create as a query, then right-click > uncheck "Enable load".)
(table as text) as table =>
let
    BaseUrl = "http://localhost:9000/gold/marts/",
    Source  = Binary.Buffer(Web.Contents(BaseUrl & table & ".parquet")),
    Parquet = Parquet.Document(Source)
in
    Parquet

// ---------------------------------------------------------------- name: fact_articles
let
    Source = Gold_Base("fact_articles"),
    Typed  = Table.TransformColumnTypes(Source, {
        {"article_id", type text},
        {"source_key", Int64.Type},
        {"date_key", Int64.Type},
        {"published_at", type datetime},
        {"title", type text},
        {"url", type text},
        {"misinfo_probability", type number},
        {"credibility_category", type text},
        {"explanation", type text},
        {"entity_count", Int64.Type},
        {"model_version", type text},
        {"model_engine", type text}
    })
in
    Typed

// ---------------------------------------------------------------- name: fact_entities
let
    Source = Gold_Base("fact_entities"),
    Typed  = Table.TransformColumnTypes(Source, {
        {"article_id", type text},
        {"entity_type", type text},
        {"entity_text", type text}
    })
in
    Typed

// ---------------------------------------------------------------- name: dim_source
let
    Source = Gold_Base("dim_source"),
    Typed  = Table.TransformColumnTypes(Source, {
        {"source_key", Int64.Type},
        {"source_name", type text}
    })
in
    Typed

// ---------------------------------------------------------------- name: dim_date
let
    Source = Gold_Base("dim_date"),
    Typed  = Table.TransformColumnTypes(Source, {
        {"date_key", Int64.Type},
        {"date", type date},
        {"year", Int64.Type},
        {"month", Int64.Type},
        {"month_name", type text},
        {"day", Int64.Type},
        {"weekday", type text},
        {"weekday_number", Int64.Type},
        {"iso_week", Int64.Type}
    })
in
    Typed

// ---------------------------------------------------------------- name: dim_credibility
let
    Source = Gold_Base("dim_credibility"),
    Typed  = Table.TransformColumnTypes(Source, {
        {"credibility_category", type text},
        {"sort_order", Int64.Type},
        {"min_probability", type number},
        {"max_probability", type number}
    })
in
    Typed