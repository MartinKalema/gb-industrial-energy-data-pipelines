with all_intervals as (
    select reporting_date, operating_timezone, interval_start_utc
    from {{ ref('int_delivery_interval_spine') }}

    union

    select reporting_date, operating_timezone, interval_start_utc
    from {{ ref('int_delivery_interval_history_spine') }}
),

dates as (
    select
        reporting_date,
        min(operating_timezone) as operating_timezone,
        count(distinct interval_start_utc) as expected_half_hour_interval_count
    from all_intervals
    group by reporting_date
)

select
    year(reporting_date) * 10000
        + month(reporting_date) * 100
        + day(reporting_date) as date_key,
    reporting_date,
    operating_timezone,
    day_of_week(reporting_date) as iso_day_of_week,
    format_datetime(cast(reporting_date as timestamp), 'EEEE') as day_name,
    date_trunc('week', reporting_date) as week_start_date,
    month(reporting_date) as month_number,
    format_datetime(cast(reporting_date as timestamp), 'MMMM') as month_name,
    quarter(reporting_date) as quarter_number,
    year(reporting_date) as year_number,
    day_of_week(reporting_date) in (6, 7) as is_weekend,
    expected_half_hour_interval_count,
    case expected_half_hour_interval_count
        when 46 then 'short_day'
        when 48 then 'standard_day'
        when 50 then 'long_day'
        else 'unexpected_day_length'
    end as daylight_saving_day_type
from dates
