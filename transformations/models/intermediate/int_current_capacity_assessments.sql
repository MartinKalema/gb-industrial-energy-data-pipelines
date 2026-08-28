select
    source_system_id,
    delivery_point_natural_id,
    interval_start_utc,
    interval_end_utc,
    assessment_status as capacity_state,
    case when assessment_status = 'final' then nameplate_ceiling_mwh_th end
        as nameplate_ceiling_mwh_th,
    case when assessment_status = 'final' then operational_restriction_mwh_th end
        as operational_restriction_mwh_th,
    case when assessment_status = 'final' then deliverable_capacity_mwh_th end
        as deliverable_capacity_mwh_th,
    quantity_unit,
    assessment_method,
    assessment_method_version,
    capacity_reason_code,
    source_capacity_revision_id,
    source_revision,
    revision_type,
    published_at_utc,
    approved_at_utc,
    finalized_at_utc,
    pipeline_run_id,
    pipeline_evidence_envelope_id,
    pipeline_ingested_at_utc,
    pipeline_raw_object_uri,
    pipeline_raw_object_sha256,
    pipeline_raw_record_locator,
    pipeline_identity_sha256,
    pipeline_payload_sha256
from {{ ref('int_capacity_assessments_knowledge_history') }}
where known_to_utc is null
