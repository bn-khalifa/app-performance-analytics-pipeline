select
    App,
    Category,
    cast(Rating as float) as rating,
    cast(Reviews as integer) as reviews,
    replace(Installs, '+', '')::integer as installs,
    case when Price > 0 then 'Paid' else 'Free' end as app_type
from {{ source('mysql', 'apps_info') }}
where rating is not null