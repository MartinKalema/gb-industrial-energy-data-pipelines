with ordered as (
    select
        delivery_interval_key,
        known_from_utc,
        known_to_utc,
        result_state_sha256,
        lag(known_to_utc) over (
            partition by delivery_interval_key
            order by known_from_utc
        ) as previous_known_to_utc,
        lag(result_state_sha256) over (
            partition by delivery_interval_key
            order by known_from_utc
        ) as previous_result_state_sha256
    from {{ ref('fct_steam_delivery_interval_history') }}
)

select *
from ordered
where known_to_utc <= known_from_utc
   or (
        previous_result_state_sha256 is not null
        and previous_known_to_utc is distinct from known_from_utc
      )
   or previous_result_state_sha256 = result_state_sha256
