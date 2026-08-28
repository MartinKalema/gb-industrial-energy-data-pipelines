select
    spine.*,

    customer.source_system_id as customer_source_system_id,
    customer.customer_version_id,
    customer.legal_name as customer_legal_name,
    customer.display_name as customer_display_name,
    customer.industry_sector_code,
    customer.country_code as customer_country_code,
    customer.lifecycle_status as customer_lifecycle_status,
    customer.tenant_authorization_scope_id,
    customer.effective_from_utc as customer_effective_from_utc,
    customer.effective_to_utc as customer_effective_to_utc,
    customer.source_revision as customer_source_revision,
    customer.revision_type as customer_revision_type,
    customer.published_at_utc as customer_published_at_utc,
    customer.approved_at_utc as customer_approved_at_utc,
    customer.pipeline_payload_sha256 as customer_pipeline_payload_sha256,

    site.source_system_id as site_source_system_id,
    site.site_version_id,
    site.site_name,
    site.locality as site_locality,
    site.postal_area as site_postal_area,
    site.country_code as site_country_code,
    site.region_code as site_region_code,
    site.iana_timezone as site_iana_timezone,
    site.operational_status as site_operational_status,
    site.latitude as site_latitude,
    site.longitude as site_longitude,
    site.effective_from_utc as site_effective_from_utc,
    site.effective_to_utc as site_effective_to_utc,
    site.source_revision as site_source_revision,
    site.revision_type as site_revision_type,
    site.published_at_utc as site_published_at_utc,
    site.approved_at_utc as site_approved_at_utc,
    site.pipeline_payload_sha256 as site_pipeline_payload_sha256,

    contract.source_system_id as contract_source_system_id,
    contract.contract_natural_id,
    contract.contract_terms_version_id,
    contract.energy_rate_gbp_per_mwh_th,
    contract.sla_penalty_rate_gbp_per_mwh_th,
    contract.currency_code,
    contract.rate_unit,
    contract.effective_from_utc as contract_effective_from_utc,
    contract.effective_to_utc as contract_effective_to_utc,
    contract.source_revision as contract_source_revision,
    contract.revision_type as contract_revision_type,
    contract.published_at_utc as contract_published_at_utc,
    contract.approved_at_utc as contract_approved_at_utc,
    contract.pipeline_payload_sha256 as contract_pipeline_payload_sha256,

    meter.source_system_id as meter_source_system_id,
    meter.meter_assignment_id,
    meter.meter_natural_id,
    meter.register_natural_id,
    meter.assignment_role as meter_assignment_role,
    meter.register_type as meter_register_type,
    meter.native_unit as meter_native_unit,
    meter.calibration_id,
    meter.maximum_plausible_30_min_change,
    meter.effective_from_utc as meter_effective_from_utc,
    meter.effective_to_utc as meter_effective_to_utc,
    meter.source_revision as meter_assignment_source_revision,
    meter.revision_type as meter_assignment_revision_type,
    meter.published_at_utc as meter_assignment_published_at_utc,
    meter.approved_at_utc as meter_assignment_approved_at_utc,
    meter.pipeline_payload_sha256 as meter_assignment_pipeline_payload_sha256
from {{ ref('int_delivery_interval_spine') }} as spine
left join {{ ref('int_current_customer_versions') }} as customer
  on spine.customer_natural_id = customer.customer_natural_id
 and customer.version_record_status = 'active'
 and customer.lifecycle_status = 'active'
 and customer.effective_from_utc <= spine.interval_start_utc
 and (
        customer.effective_to_utc is null
        or spine.interval_end_utc <= customer.effective_to_utc
     )
left join {{ ref('int_current_site_versions') }} as site
  on spine.site_natural_id = site.site_natural_id
 and site.version_record_status = 'active'
 and site.operational_status = 'operational'
 and site.effective_from_utc <= spine.interval_start_utc
 and (
        site.effective_to_utc is null
        or spine.interval_end_utc <= site.effective_to_utc
     )
left join {{ ref('int_current_contract_terms') }} as contract
  on spine.delivery_point_natural_id = contract.delivery_point_natural_id
 and spine.customer_natural_id = contract.customer_natural_id
 and contract.terms_status = 'active'
 and contract.effective_from_utc <= spine.interval_start_utc
 and (
        contract.effective_to_utc is null
        or spine.interval_end_utc <= contract.effective_to_utc
     )
left join {{ ref('int_current_meter_assignments') }} as meter
  on spine.delivery_point_natural_id = meter.delivery_point_natural_id
 and meter.assignment_status = 'active'
 and meter.assignment_role = 'authoritative_revenue'
 and meter.register_type = 'thermal_energy'
 and meter.effective_from_utc <= spine.interval_start_utc
 and (
        meter.effective_to_utc is null
        or spine.interval_end_utc <= meter.effective_to_utc
     )
