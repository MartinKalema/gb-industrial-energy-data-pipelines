with approved_revisions as (
    select
        *,
        row_number() over (
            partition by
                source_system_id,
                meter_natural_id,
                register_natural_id,
                reading_at_utc
            order by source_revision desc
        ) as revision_rank
    from {{ ref('stg_validated__revenue_meter_reading') }}
    where approval_state = 'approved'
      and approved_at_utc is not null
)

select *
from approved_revisions
where revision_rank = 1
