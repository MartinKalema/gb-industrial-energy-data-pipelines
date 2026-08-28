with intervals as (
    select distinct
        reporting_date,
        operating_timezone,
        interval_start_utc,
        interval_end_utc,
        local_period_number
    from {{ ref('int_delivery_interval_spine') }}

    union

    select distinct
        reporting_date,
        operating_timezone,
        interval_start_utc,
        interval_end_utc,
        local_period_number
    from {{ ref('int_delivery_interval_history_spine') }}
)

select
    {{ sha256_key(['to_iso8601(interval_start_utc)']) }} as interval_key,
    year(reporting_date) * 10000
        + month(reporting_date) * 100
        + day(reporting_date) as date_key,
    reporting_date,
    interval_start_utc,
    interval_end_utc,
    cast(at_timezone(interval_start_utc, operating_timezone) as timestamp(6))
        as interval_start_local,
    cast(at_timezone(interval_end_utc, operating_timezone) as timestamp(6))
        as interval_end_local,
    operating_timezone,
    timezone_hour(at_timezone(interval_start_utc, operating_timezone)) * 60
        + timezone_minute(at_timezone(interval_start_utc, operating_timezone))
        as utc_offset_minutes,
    local_period_number,
    timezone_hour(at_timezone(interval_start_utc, operating_timezone)) <> 0
        as is_daylight_saving_time
from intervals
