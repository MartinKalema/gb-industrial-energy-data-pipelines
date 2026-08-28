with approved_revisions as (
    select
        *,
        row_number() over (
            partition by
                source_system_id,
                delivery_point_natural_id,
                interval_start_utc
            order by source_revision desc
        ) as revision_rank
    from {{ ref('stg_validated__commitment_schedule') }}
    where approval_state = 'approved'
      and approved_at_utc is not null
)

select *
from approved_revisions
where revision_rank = 1
