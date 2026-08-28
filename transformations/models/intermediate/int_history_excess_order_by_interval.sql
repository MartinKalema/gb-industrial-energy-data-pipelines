select
    points.delivery_interval_key,
    points.knowledge_point_utc,
    count(history.order_interval_line_id) as eligible_order_state_count,
    case
        when count(history.order_interval_line_id) = 0
            then cast(decimal '0.000000' as decimal(20, 6))
        when count(history.order_interval_line_id) = 1
         and max(history.order_state) = 'approved'
            then max(history.approved_extra_mwh_th)
        when count(history.order_interval_line_id) = 1
         and max(history.order_state) = 'cancelled'
            then cast(decimal '0.000000' as decimal(20, 6))
    end as approved_extra_mwh_th,
    case when count(history.order_interval_line_id) = 1
        then max(history.order_state) end as order_state,
    case when count(history.order_interval_line_id) = 1
        then max(history.excess_order_natural_id) end as excess_order_natural_id,
    case when count(history.order_interval_line_id) = 1
        then max(history.order_interval_line_id) end as order_interval_line_id,
    case when count(history.order_interval_line_id) = 1
        then max(history.source_revision) end as excess_order_source_revision,
    case when count(history.order_interval_line_id) = 1
        then max(history.published_at_utc) end as excess_order_published_at_utc,
    case when count(history.order_interval_line_id) = 1
        then max(history.approved_at_utc) end as excess_order_approved_at_utc,
    case when count(history.order_interval_line_id) = 1
        then max(history.pipeline_payload_sha256) end
        as excess_order_pipeline_payload_sha256
from {{ ref('int_delivery_interval_knowledge_change_points') }} as points
left join {{ ref('int_eligible_excess_orders_knowledge_history') }} as history
  on points.delivery_point_natural_id = history.delivery_point_natural_id
 and points.interval_start_utc = history.interval_start_utc
 and points.interval_end_utc = history.interval_end_utc
 and history.known_from_utc <= points.knowledge_point_utc
 and (
        history.known_to_utc is null
        or points.knowledge_point_utc < history.known_to_utc
     )
group by
    points.delivery_interval_key,
    points.knowledge_point_utc
