with approved_revisions as (
    select
        *,
        row_number() over (
            partition by source_system_id, site_version_id
            order by source_revision desc
        ) as revision_rank
    from {{ ref('stg_validated__industrial_site_master') }}
    where approval_state = 'approved'
      and approved_at_utc is not null
)

select *
from approved_revisions
where revision_rank = 1
