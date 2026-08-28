with active_assignments as (
    select
        source_system_id,
        delivery_point_assignment_id,
        delivery_point_natural_id,
        site_natural_id,
        effective_from_utc,
        effective_to_utc
    from {{ ref('int_current_delivery_point_assignments') }}
    where assignment_status = 'active'
),

overlapping_site_assignments as (
    select
        left_assignment.source_system_id,
        left_assignment.site_natural_id,
        left_assignment.delivery_point_assignment_id as left_assignment_id,
        right_assignment.delivery_point_assignment_id as right_assignment_id
    from active_assignments as left_assignment
    inner join active_assignments as right_assignment
      on left_assignment.source_system_id = right_assignment.source_system_id
     and left_assignment.site_natural_id = right_assignment.site_natural_id
     and left_assignment.delivery_point_assignment_id
            < right_assignment.delivery_point_assignment_id
     and left_assignment.effective_from_utc
            < coalesce(
                right_assignment.effective_to_utc,
                timestamp '9999-12-31 23:59:59.999999 UTC'
            )
     and right_assignment.effective_from_utc
            < coalesce(
                left_assignment.effective_to_utc,
                timestamp '9999-12-31 23:59:59.999999 UTC'
            )
)

select *
from overlapping_site_assignments
