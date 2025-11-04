{{ config(materialized='table') }}

select distinct
    app_name,
    category,
    content_rating,
    type
    genres,
from {{ ref('stg_apps') }}
where app_name is not null
