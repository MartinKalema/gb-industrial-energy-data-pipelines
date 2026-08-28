with cutoff_eligible_revisions as (
    select
        *,
        row_number() over (
            partition by source_system_id, order_interval_line_id
            order by source_revision desc
        ) as revision_rank
    from {{ ref('stg_validated__approved_excess_order') }}
    where approval_state = 'approved'
      and approved_at_utc is not null
      and approved_at_utc < interval_start_utc
      and published_at_utc < interval_start_utc
),

current_lines as (
    select *
    from cutoff_eligible_revisions
    where revision_rank = 1
)

select *
from current_lines
