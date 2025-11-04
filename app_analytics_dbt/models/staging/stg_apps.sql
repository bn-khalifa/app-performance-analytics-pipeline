{{ config(materialized='view') }}

SELECT
    trim("App") AS app_name,
    trim("Category") AS category,
    "Rating" AS rating,
    "Reviews" AS reviews_count,
    trim("Size") AS size,
    -- clean installs "10,000+" → 10000
    CASE
        WHEN "Installs" ilike '%M%' THEN cast(replace(replace("Installs", 'M+', ''), ',', '') AS float) * 1e6
        WHEN "Installs" ilike '%K%' THEN cast(replace(replace("Installs", 'K+', ''), ',', '') AS float) * 1e3
        WHEN "Installs" ilike '%nan%' OR "Installs" = '' THEN NULL
        ELSE cast(replace(replace("Installs", '+', ''), ',', '') AS float)
    END AS installs_count,
    CASE 
        WHEN "Type" = '0' THEN 'Free'
        WHEN "Type" = 'NaN' THEN NULL
        ELSE trim("Type")
    END AS type,
    "Price" AS price_usd,
    trim("Content_Rating") AS content_rating,
    trim("Genres") AS genres
FROM {{ source('raw', 'apps_info') }}
WHERE "App" IS NOT NULL