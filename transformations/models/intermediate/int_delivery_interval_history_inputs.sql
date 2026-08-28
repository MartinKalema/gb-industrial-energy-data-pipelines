select
    boundaries.*,

    commitment.source_commitment_revision_id
        as history_source_commitment_revision_id,
    commitment.source_revision as history_commitment_source_revision,
    commitment.schedule_record_status as history_schedule_record_status,
    commitment.obligation_status as history_obligation_status,
    case when commitment.schedule_record_status = 'active'
        then commitment.committed_mwh_th end as history_committed_mwh_th,
    commitment.published_at_utc as history_commitment_published_at_utc,
    commitment.approved_at_utc as history_commitment_approved_at_utc,
    commitment.pipeline_payload_sha256 as history_commitment_pipeline_payload_sha256,

    orders.eligible_order_state_count as history_eligible_order_state_count,
    orders.approved_extra_mwh_th as history_approved_extra_mwh_th,
    orders.order_state as history_order_state,
    orders.excess_order_natural_id as history_excess_order_natural_id,
    orders.order_interval_line_id as history_order_interval_line_id,
    orders.excess_order_source_revision as history_excess_order_source_revision,
    orders.excess_order_published_at_utc as history_excess_order_published_at_utc,
    orders.excess_order_approved_at_utc as history_excess_order_approved_at_utc,
    orders.excess_order_pipeline_payload_sha256
        as history_excess_order_pipeline_payload_sha256,

    coalesce(capacity.assessment_status, 'missing') as history_capacity_status,
    case when capacity.assessment_status = 'final'
        then capacity.deliverable_capacity_mwh_th end
        as history_deliverable_capacity_mwh_th,
    capacity.capacity_reason_code as history_capacity_reason_code,
    capacity.source_capacity_revision_id as history_source_capacity_revision_id,
    capacity.source_revision as history_capacity_source_revision,
    capacity.published_at_utc as history_capacity_published_at_utc,
    capacity.approved_at_utc as history_capacity_approved_at_utc,
    capacity.pipeline_payload_sha256 as history_capacity_pipeline_payload_sha256
from {{ ref('int_delivery_interval_history_boundaries') }} as boundaries
left join {{ ref('int_commitments_knowledge_history') }} as commitment
  on boundaries.delivery_point_natural_id = commitment.delivery_point_natural_id
 and boundaries.interval_start_utc = commitment.interval_start_utc
 and boundaries.interval_end_utc = commitment.interval_end_utc
 and commitment.known_from_utc <= boundaries.knowledge_point_utc
 and (commitment.known_to_utc is null or boundaries.knowledge_point_utc < commitment.known_to_utc)
left join {{ ref('int_history_excess_order_by_interval') }} as orders
  on boundaries.delivery_interval_key = orders.delivery_interval_key
 and boundaries.knowledge_point_utc = orders.knowledge_point_utc
left join {{ ref('int_capacity_assessments_knowledge_history') }} as capacity
  on boundaries.delivery_point_natural_id = capacity.delivery_point_natural_id
 and boundaries.interval_start_utc = capacity.interval_start_utc
 and boundaries.interval_end_utc = capacity.interval_end_utc
 and capacity.known_from_utc <= boundaries.knowledge_point_utc
 and (capacity.known_to_utc is null or boundaries.knowledge_point_utc < capacity.known_to_utc)
