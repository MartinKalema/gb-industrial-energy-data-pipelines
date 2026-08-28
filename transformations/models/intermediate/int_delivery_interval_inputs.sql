select
    boundaries.*,

    commitment.source_system_id as commitment_source_system_id,
    commitment.source_commitment_revision_id,
    commitment.contract_natural_id as commitment_contract_natural_id,
    commitment.schedule_record_status,
    commitment.obligation_status,
    case when commitment.schedule_record_status = 'active'
        then commitment.committed_mwh_th end as committed_mwh_th,
    commitment.commitment_reason_code,
    commitment.source_revision as commitment_source_revision,
    commitment.revision_type as commitment_revision_type,
    commitment.published_at_utc as commitment_published_at_utc,
    commitment.approved_at_utc as commitment_approved_at_utc,
    commitment.pipeline_payload_sha256 as commitment_pipeline_payload_sha256,

    excess.eligible_order_count,
    excess.order_state as excess_order_state,
    excess.approved_extra_mwh_th as source_approved_extra_mwh_th,
    excess.excess_order_natural_id,
    excess.order_interval_line_id,
    excess.excess_order_source_revision,
    excess.excess_order_revision_type,
    excess.excess_order_published_at_utc,
    excess.excess_order_approved_at_utc,
    excess.excess_order_pipeline_payload_sha256,

    capacity.capacity_state,
    capacity.nameplate_ceiling_mwh_th,
    capacity.operational_restriction_mwh_th,
    capacity.deliverable_capacity_mwh_th,
    capacity.capacity_reason_code,
    capacity.source_capacity_revision_id,
    capacity.source_revision as capacity_source_revision,
    capacity.revision_type as capacity_revision_type,
    capacity.published_at_utc as capacity_published_at_utc,
    capacity.approved_at_utc as capacity_approved_at_utc,
    capacity.pipeline_payload_sha256 as capacity_pipeline_payload_sha256
from {{ ref('int_delivery_interval_boundaries') }} as boundaries
left join {{ ref('int_current_commitments') }} as commitment
  on boundaries.delivery_point_natural_id = commitment.delivery_point_natural_id
 and boundaries.interval_start_utc = commitment.interval_start_utc
 and boundaries.interval_end_utc = commitment.interval_end_utc
left join {{ ref('int_current_excess_order_by_interval') }} as excess
  on boundaries.delivery_point_natural_id = excess.delivery_point_natural_id
 and boundaries.interval_start_utc = excess.interval_start_utc
 and boundaries.interval_end_utc = excess.interval_end_utc
left join {{ ref('int_current_capacity_assessments') }} as capacity
  on boundaries.delivery_point_natural_id = capacity.delivery_point_natural_id
 and boundaries.interval_start_utc = capacity.interval_start_utc
 and boundaries.interval_end_utc = capacity.interval_end_utc
