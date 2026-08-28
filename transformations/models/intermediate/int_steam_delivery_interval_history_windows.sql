with ordered_states as (
    select
        states.*,
        lag(result_state_sha256) over (
            partition by delivery_interval_key
            order by knowledge_point_utc
        ) as previous_result_state_sha256
    from {{ ref('int_steam_delivery_interval_history_states') }} as states
),

changes as (
    select *
    from ordered_states
    where previous_result_state_sha256 is null
       or result_state_sha256 <> previous_result_state_sha256
)

select
    changes.*,
    knowledge_point_utc as known_from_utc,
    lead(knowledge_point_utc) over (
        partition by delivery_interval_key
        order by knowledge_point_utc
    ) as known_to_utc
from changes
