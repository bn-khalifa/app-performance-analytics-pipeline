{{ config(materialized='table') }}

SELECT
    a.app_name,
    a.category,
    TRY_CAST(a.rating AS DOUBLE) AS rating,
    a.type AS type, 
    COALESCE(TRY_CAST(a.price_usd AS DOUBLE), 0.0) AS price_usd,
    TRY_CAST(a.reviews_count AS BIGINT) AS reviews_count,
    TRY_CAST(a.installs_count AS DOUBLE) AS installs_count,

    AVG(TRY_CAST(r.sentiment_polarity AS DOUBLE)) AS avg_sentiment_polarity,
    AVG(TRY_CAST(r.sentiment_subjectivity AS DOUBLE)) AS avg_sentiment_subjectivity,
    COUNT(r.sentiment) AS sentiment_count
FROM {{ ref('stg_apps') }} a
LEFT JOIN {{ ref('stg_reviews') }} r
    ON a.app_name = r.app_name
GROUP BY
    a.app_name, 
    a.category, 
    a.rating, 
    a.type, 
    a.price_usd,
    a.reviews_count, 
    a.installs_count