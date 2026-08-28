with correction as (
    select
        max(greatest(published_at_utc, approved_at_utc))
            as correction_known_from_utc,
        count(*) as correction_row_count
    from {{ ref('stg_validated__revenue_meter_reading') }}
    where meter_natural_id = 'RM-001'
      and register_natural_id = 'ENERGY-01'
      and reading_at_utc = timestamp '2026-08-26 03:30:00 UTC'
      and source_revision = 2
),
expected (interval_start_utc, delivered_mwh_th) as (
    values
        (timestamp '2026-08-26 03:00:00 UTC', decimal '4.900000'),
        (timestamp '2026-08-26 03:30:00 UTC', decimal '5.100000')
)

select expected.*
from expected
cross join correction
left join {{ ref('fct_steam_delivery_interval_history') }} as history
  on history.delivery_point_natural_id = 'DP-001'
 and history.interval_start_utc = expected.interval_start_utc
 and history.known_from_utc = correction.correction_known_from_utc
where history.delivery_interval_history_key is null
   or correction.correction_row_count <> 1
   or history.delivered_mwh_th is distinct from expected.delivered_mwh_th
