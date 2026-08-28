select
    delivery_point_natural_id,
    interval_start_utc,
    interval_end_utc,
    count(*) as eligible_order_count,
    case
        when count(*) = 1 and max(order_state) = 'approved'
            then max(approved_extra_mwh_th)
        when count(*) = 1 and max(order_state) = 'cancelled'
            then cast(decimal '0.000000' as decimal(20, 6))
    end as approved_extra_mwh_th,
    case when count(*) = 1 then max(order_state) end as order_state,
    case when count(*) = 1 then max(excess_order_natural_id) end
        as excess_order_natural_id,
    case when count(*) = 1 then max(order_interval_line_id) end
        as order_interval_line_id,
    case when count(*) = 1 then max(source_revision) end
        as excess_order_source_revision,
    case when count(*) = 1 then max(revision_type) end
        as excess_order_revision_type,
    case when count(*) = 1 then max(published_at_utc) end
        as excess_order_published_at_utc,
    case when count(*) = 1 then max(approved_at_utc) end
        as excess_order_approved_at_utc,
    case when count(*) = 1 then max(pipeline_payload_sha256) end
        as excess_order_pipeline_payload_sha256
from {{ ref('int_current_eligible_excess_orders') }}
group by
    delivery_point_natural_id,
    interval_start_utc,
    interval_end_utc
