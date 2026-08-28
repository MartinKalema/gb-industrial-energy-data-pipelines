select
    points.*,

    assignment.source_system_id as history_delivery_point_source_system_id,
    assignment.delivery_point_assignment_id as history_delivery_point_assignment_id,
    assignment.delivery_point_name as history_delivery_point_name,
    assignment.site_natural_id as history_site_natural_id,
    assignment.customer_natural_id as history_customer_natural_id,
    assignment.service_type as history_service_type,
    assignment.source_revision as history_delivery_point_assignment_source_revision,
    assignment.published_at_utc as history_delivery_point_assignment_published_at_utc,
    assignment.approved_at_utc as history_delivery_point_assignment_approved_at_utc,
    assignment.pipeline_payload_sha256
        as history_delivery_point_assignment_pipeline_payload_sha256,

    customer.source_system_id as history_customer_source_system_id,
    customer.customer_version_id as history_customer_version_id,
    customer.source_revision as history_customer_source_revision,
    customer.published_at_utc as history_customer_published_at_utc,
    customer.approved_at_utc as history_customer_approved_at_utc,
    customer.pipeline_payload_sha256 as history_customer_pipeline_payload_sha256,

    site.source_system_id as history_site_source_system_id,
    site.site_version_id as history_site_version_id,
    site.source_revision as history_site_source_revision,
    site.published_at_utc as history_site_published_at_utc,
    site.approved_at_utc as history_site_approved_at_utc,
    site.pipeline_payload_sha256 as history_site_pipeline_payload_sha256,

    contract.source_system_id as history_contract_source_system_id,
    contract.contract_natural_id as history_contract_natural_id,
    contract.contract_terms_version_id as history_contract_terms_version_id,
    contract.energy_rate_gbp_per_mwh_th as history_energy_rate_gbp_per_mwh_th,
    contract.sla_penalty_rate_gbp_per_mwh_th
        as history_sla_penalty_rate_gbp_per_mwh_th,
    contract.currency_code as history_currency_code,
    contract.source_revision as history_contract_source_revision,
    contract.published_at_utc as history_contract_published_at_utc,
    contract.approved_at_utc as history_contract_approved_at_utc,
    contract.pipeline_payload_sha256 as history_contract_pipeline_payload_sha256,

    meter.source_system_id as history_meter_source_system_id,
    meter.meter_assignment_id as history_meter_assignment_id,
    meter.meter_natural_id as history_meter_natural_id,
    meter.register_natural_id as history_register_natural_id,
    meter.native_unit as history_meter_native_unit,
    meter.maximum_plausible_30_min_change
        as history_maximum_plausible_30_min_change,
    meter.source_revision as history_meter_assignment_source_revision,
    meter.published_at_utc as history_meter_assignment_published_at_utc,
    meter.approved_at_utc as history_meter_assignment_approved_at_utc,
    meter.pipeline_payload_sha256 as history_meter_assignment_pipeline_payload_sha256
from {{ ref('int_delivery_interval_knowledge_change_points') }} as points
left join {{ ref('int_delivery_point_assignments_knowledge_history') }} as assignment
  on points.delivery_point_natural_id = assignment.delivery_point_natural_id
 and assignment.known_from_utc <= points.knowledge_point_utc
 and (assignment.known_to_utc is null or points.knowledge_point_utc < assignment.known_to_utc)
 and assignment.assignment_status = 'active'
 and assignment.effective_from_utc <= points.interval_start_utc
 and (assignment.effective_to_utc is null or points.interval_end_utc <= assignment.effective_to_utc)
left join {{ ref('int_customer_versions_knowledge_history') }} as customer
  on assignment.customer_natural_id = customer.customer_natural_id
 and customer.known_from_utc <= points.knowledge_point_utc
 and (customer.known_to_utc is null or points.knowledge_point_utc < customer.known_to_utc)
 and customer.version_record_status = 'active'
 and customer.lifecycle_status = 'active'
 and customer.effective_from_utc <= points.interval_start_utc
 and (customer.effective_to_utc is null or points.interval_end_utc <= customer.effective_to_utc)
left join {{ ref('int_site_versions_knowledge_history') }} as site
  on assignment.site_natural_id = site.site_natural_id
 and site.known_from_utc <= points.knowledge_point_utc
 and (site.known_to_utc is null or points.knowledge_point_utc < site.known_to_utc)
 and site.version_record_status = 'active'
 and site.operational_status = 'operational'
 and site.effective_from_utc <= points.interval_start_utc
 and (site.effective_to_utc is null or points.interval_end_utc <= site.effective_to_utc)
left join {{ ref('int_contract_terms_knowledge_history') }} as contract
  on assignment.delivery_point_natural_id = contract.delivery_point_natural_id
 and assignment.customer_natural_id = contract.customer_natural_id
 and contract.known_from_utc <= points.knowledge_point_utc
 and (contract.known_to_utc is null or points.knowledge_point_utc < contract.known_to_utc)
 and contract.terms_status = 'active'
 and contract.effective_from_utc <= points.interval_start_utc
 and (contract.effective_to_utc is null or points.interval_end_utc <= contract.effective_to_utc)
left join {{ ref('int_meter_assignments_knowledge_history') }} as meter
  on assignment.delivery_point_natural_id = meter.delivery_point_natural_id
 and meter.known_from_utc <= points.knowledge_point_utc
 and (meter.known_to_utc is null or points.knowledge_point_utc < meter.known_to_utc)
 and meter.assignment_status = 'active'
 and meter.assignment_role = 'authoritative_revenue'
 and meter.register_type = 'thermal_energy'
 and meter.effective_from_utc <= points.interval_start_utc
 and (meter.effective_to_utc is null or points.interval_end_utc <= meter.effective_to_utc)
