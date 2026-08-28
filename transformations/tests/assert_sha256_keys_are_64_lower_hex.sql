with keys as (
    select cast(customer_key as varchar) as key_value from {{ ref('dim_customer') }}
    union all select cast(site_key as varchar) from {{ ref('dim_site') }}
    union all select cast(delivery_point_key as varchar) from {{ ref('dim_delivery_point') }}
    union all select cast(contract_key as varchar) from {{ ref('dim_contract') }}
    union all select cast(meter_key as varchar) from {{ ref('dim_meter') }}
    union all select cast(interval_key as varchar) from {{ ref('dim_interval') }}
    union all select cast(data_status_key as varchar) from {{ ref('dim_data_status') }}
    union all select cast(customer_revision_key as varchar) from {{ ref('dim_customer_revision_audit') }}
    union all select cast(site_revision_key as varchar) from {{ ref('dim_site_revision_audit') }}
    union all select cast(delivery_point_revision_key as varchar) from {{ ref('dim_delivery_point_revision_audit') }}
    union all select cast(contract_revision_key as varchar) from {{ ref('dim_contract_revision_audit') }}
    union all select cast(meter_revision_key as varchar) from {{ ref('dim_meter_revision_audit') }}
    union all select cast(delivery_interval_key as varchar) from {{ ref('fct_steam_delivery_interval') }}
    union all select cast(delivery_interval_history_key as varchar) from {{ ref('fct_steam_delivery_interval_history') }}
)

select key_value
from keys
where key_value is null
   or not regexp_like(key_value, '^[0-9a-f]{64}$')
