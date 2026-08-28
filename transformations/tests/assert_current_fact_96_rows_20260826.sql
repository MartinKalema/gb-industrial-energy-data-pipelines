select
    count(*) as actual_row_count
from {{ ref('fct_steam_delivery_interval') }}
where date_key = 20260826
having count(*) <> 96
