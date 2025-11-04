{{ config(materialized='view') }}

select
    trim("App") as app_name,
    trim("Translated_Review") as translated_review,
    trim("Sentiment") as sentiment,
    coalesce(try_cast(nullif("Sentiment_Polarity", 'nan') as float), 0.0) as sentiment_polarity,
    coalesce(try_cast(nullif("Sentiment_Subjectivity", 'nan') as float), 0.0) as sentiment_subjectivity
from {{ source('raw', 'reviews') }}
where "App" is not null
  and sentiment is not null