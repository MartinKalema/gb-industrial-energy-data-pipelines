select
    {{ sha256_key([
        'delivery_point_natural_id',
        'to_iso8601(interval_start_utc)',
        'to_iso8601(known_from_utc)'
    ]) }} as delivery_interval_history_key,
    delivery_interval_key,
    history_date_key as date_key,
    history_interval_key as interval_key,
    history_customer_key as customer_key,
    history_site_key as site_key,
    history_delivery_point_key as delivery_point_key,
    history_contract_key as contract_key,
    history_meter_key as meter_key,
    history_data_status_key as data_status_key,
    history_customer_revision_key as customer_revision_key,
    history_site_revision_key as site_revision_key,
    history_delivery_point_revision_key as delivery_point_revision_key,
    history_contract_revision_key as contract_revision_key,
    history_meter_revision_key as meter_revision_key,

    delivery_point_natural_id,
    history_customer_natural_id as customer_natural_id,
    history_site_natural_id as site_natural_id,
    history_contract_natural_id as contract_natural_id,
    history_meter_natural_id as meter_natural_id,
    history_register_natural_id as register_natural_id,
    interval_start_utc,
    interval_end_utc,
    known_from_utc,
    known_to_utc,
    result_state_sha256,

    cast(history_opening_register_mwh_th as decimal(20, 6))
        as opening_register_mwh_th,
    cast(history_closing_register_mwh_th as decimal(20, 6))
        as closing_register_mwh_th,
    cast(history_committed_mwh_th as decimal(20, 6)) as committed_mwh_th,
    cast(history_delivered_mwh_th as decimal(20, 6)) as delivered_mwh_th,
    cast(history_delivered_steam_t as decimal(20, 6)) as delivered_steam_t,
    cast(history_shortfall_mwh_th as decimal(20, 6)) as shortfall_mwh_th,
    cast(history_excess_mwh_th as decimal(20, 6)) as excess_mwh_th,
    cast(history_deliverable_capacity_mwh_th as decimal(20, 6))
        as deliverable_capacity_mwh_th,
    cast(history_billable_mwh_th as decimal(20, 6)) as billable_mwh_th,
    cast(history_gross_earned_revenue_gbp as decimal(38, 12))
        as gross_earned_revenue_gbp,
    cast(history_accrued_sla_penalty_gbp as decimal(38, 12))
        as accrued_sla_penalty_gbp,
    cast(history_net_earned_revenue_gbp as decimal(38, 12))
        as net_earned_revenue_gbp,

    cast(history_approved_extra_mwh_th as decimal(20, 6)) as approved_extra_mwh_th,
    cast(history_unbilled_excess_mwh_th as decimal(20, 6)) as unbilled_excess_mwh_th,
    cast(history_sla_attainment_numerator_mwh_th as decimal(20, 6))
        as sla_attainment_numerator_mwh_th,
    cast(history_contractual_availability_numerator_mwh_th as decimal(20, 6))
        as contractual_availability_numerator_mwh_th,
    history_expected_interval_count as expected_interval_count,
    history_commitment_record_count as commitment_record_count,
    history_applicable_interval_count as applicable_interval_count,
    history_accepted_applicable_delivery_count as accepted_applicable_delivery_count,
    history_final_applicable_capacity_count as final_applicable_capacity_count,

    cast(history_energy_rate_gbp_per_mwh_th as decimal(18, 6))
        as energy_rate_gbp_per_mwh_th,
    cast(history_sla_penalty_rate_gbp_per_mwh_th as decimal(18, 6))
        as sla_penalty_rate_gbp_per_mwh_th,
    history_currency_code as currency_code,
    history_delivery_measurement_status as delivery_measurement_status,
    history_commitment_status as commitment_status,
    history_capacity_status as capacity_status,
    history_contract_status as contract_status,
    history_customer_access_status as customer_access_status,
    history_sla_result_status as sla_result_status,
    history_availability_result_status as availability_result_status,
    history_financial_result_status as financial_result_status,
    history_correction_status as correction_status,

    coverage_run_count,
    coverage_pipeline_run_ids,
    first_coverage_published_at_utc,
    latest_coverage_published_at_utc,
    coverage_raw_manifest_uris,
    coverage_raw_manifest_sha256s,
    coverage_reconciliation_artifact_uris,
    coverage_reconciliation_artifact_sha256s,

    history_customer_version_id as customer_version_id,
    history_customer_source_revision as customer_source_revision,
    history_customer_published_at_utc as customer_published_at_utc,
    history_customer_approved_at_utc as customer_approved_at_utc,
    history_customer_pipeline_payload_sha256 as customer_pipeline_payload_sha256,
    history_site_version_id as site_version_id,
    history_site_source_revision as site_source_revision,
    history_site_published_at_utc as site_published_at_utc,
    history_site_approved_at_utc as site_approved_at_utc,
    history_site_pipeline_payload_sha256 as site_pipeline_payload_sha256,
    history_delivery_point_assignment_id as delivery_point_assignment_id,
    history_delivery_point_assignment_source_revision
        as delivery_point_assignment_source_revision,
    history_delivery_point_assignment_published_at_utc
        as delivery_point_assignment_published_at_utc,
    history_delivery_point_assignment_approved_at_utc
        as delivery_point_assignment_approved_at_utc,
    history_delivery_point_assignment_pipeline_payload_sha256
        as delivery_point_assignment_pipeline_payload_sha256,
    history_contract_terms_version_id as contract_terms_version_id,
    history_contract_source_revision as contract_source_revision,
    history_contract_published_at_utc as contract_published_at_utc,
    history_contract_approved_at_utc as contract_approved_at_utc,
    history_contract_pipeline_payload_sha256 as contract_pipeline_payload_sha256,
    history_meter_assignment_id as meter_assignment_id,
    history_meter_assignment_source_revision as meter_assignment_source_revision,
    history_meter_assignment_published_at_utc as meter_assignment_published_at_utc,
    history_meter_assignment_approved_at_utc as meter_assignment_approved_at_utc,
    history_meter_assignment_pipeline_payload_sha256
        as meter_assignment_pipeline_payload_sha256,
    history_source_commitment_revision_id as source_commitment_revision_id,
    history_commitment_source_revision as commitment_source_revision,
    history_commitment_published_at_utc as commitment_published_at_utc,
    history_commitment_approved_at_utc as commitment_approved_at_utc,
    history_commitment_pipeline_payload_sha256 as commitment_pipeline_payload_sha256,
    history_excess_order_natural_id as excess_order_natural_id,
    history_order_interval_line_id as order_interval_line_id,
    history_excess_order_source_revision as excess_order_source_revision,
    history_excess_order_published_at_utc as excess_order_published_at_utc,
    history_excess_order_approved_at_utc as excess_order_approved_at_utc,
    history_excess_order_pipeline_payload_sha256
        as excess_order_pipeline_payload_sha256,
    history_opening_source_reading_revision_id as opening_source_reading_revision_id,
    history_opening_reading_source_revision as opening_reading_source_revision,
    history_opening_reading_published_at_utc as opening_reading_published_at_utc,
    history_opening_reading_approved_at_utc as opening_reading_approved_at_utc,
    history_opening_reading_pipeline_payload_sha256
        as opening_reading_pipeline_payload_sha256,
    history_closing_source_reading_revision_id as closing_source_reading_revision_id,
    history_closing_reading_source_revision as closing_reading_source_revision,
    history_closing_reading_published_at_utc as closing_reading_published_at_utc,
    history_closing_reading_approved_at_utc as closing_reading_approved_at_utc,
    history_closing_reading_pipeline_payload_sha256
        as closing_reading_pipeline_payload_sha256,
    history_source_capacity_revision_id as source_capacity_revision_id,
    history_capacity_source_revision as capacity_source_revision,
    history_capacity_published_at_utc as capacity_published_at_utc,
    history_capacity_approved_at_utc as capacity_approved_at_utc,
    history_capacity_pipeline_payload_sha256 as capacity_pipeline_payload_sha256
from {{ ref('int_steam_delivery_interval_history_windows') }}
