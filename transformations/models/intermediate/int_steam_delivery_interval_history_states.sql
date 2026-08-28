with enriched as (
    select
        calculations.*,
        year(reporting_date) * 10000
            + month(reporting_date) * 100
            + day(reporting_date) as history_date_key,
        {{ sha256_key(['to_iso8601(interval_start_utc)']) }} as history_interval_key,
        {{ sha256_key([
            'history_customer_source_system_id',
            'history_customer_version_id'
        ]) }} as history_customer_key,
        {{ sha256_key([
            'history_site_source_system_id',
            'history_site_version_id'
        ]) }} as history_site_key,
        {{ sha256_key([
            'history_delivery_point_source_system_id',
            'history_delivery_point_assignment_id'
        ]) }} as history_delivery_point_key,
        {{ sha256_key([
            'history_contract_source_system_id',
            'history_contract_terms_version_id'
        ]) }} as history_contract_key,
        {{ sha256_key([
            'history_meter_source_system_id',
            'history_meter_assignment_id'
        ]) }} as history_meter_key,
        {{ sha256_key([
            'history_customer_source_system_id',
            'history_customer_version_id',
            'cast(history_customer_source_revision as varchar)'
        ]) }} as history_customer_revision_key,
        {{ sha256_key([
            'history_site_source_system_id',
            'history_site_version_id',
            'cast(history_site_source_revision as varchar)'
        ]) }} as history_site_revision_key,
        {{ sha256_key([
            'history_delivery_point_source_system_id',
            'history_delivery_point_assignment_id',
            'cast(history_delivery_point_assignment_source_revision as varchar)'
        ]) }} as history_delivery_point_revision_key,
        {{ sha256_key([
            'history_contract_source_system_id',
            'history_contract_terms_version_id',
            'cast(history_contract_source_revision as varchar)'
        ]) }} as history_contract_revision_key,
        {{ sha256_key([
            'history_meter_source_system_id',
            'history_meter_assignment_id',
            'cast(history_meter_assignment_source_revision as varchar)'
        ]) }} as history_meter_revision_key
    from {{ ref('int_steam_delivery_interval_history_calculations') }} as calculations
),

with_status_key as (
    select
        enriched.*,
        {{ sha256_key([
            'history_delivery_measurement_status',
            'history_commitment_status',
            'history_capacity_status',
            'history_contract_status',
            'history_customer_access_status',
            'history_sla_result_status',
            'history_availability_result_status',
            'history_financial_result_status',
            'history_correction_status'
        ]) }} as history_data_status_key
    from enriched
)

select
    with_status_key.*,
    {{ sha256_key([
        "coalesce(history_customer_version_id, '<null>')",
        "coalesce(history_site_version_id, '<null>')",
        "coalesce(history_delivery_point_assignment_id, '<null>')",
        "coalesce(history_contract_terms_version_id, '<null>')",
        "coalesce(history_meter_assignment_id, '<null>')",
        "coalesce(history_opening_source_reading_revision_id, '<null>')",
        "coalesce(history_closing_source_reading_revision_id, '<null>')",
        "coalesce(history_source_commitment_revision_id, '<null>')",
        "coalesce(history_order_interval_line_id, '<null>')",
        "coalesce(history_source_capacity_revision_id, '<null>')",
        "coalesce(history_delivery_point_assignment_pipeline_payload_sha256, '<null>')",
        "coalesce(history_customer_pipeline_payload_sha256, '<null>')",
        "coalesce(history_site_pipeline_payload_sha256, '<null>')",
        "coalesce(history_contract_pipeline_payload_sha256, '<null>')",
        "coalesce(history_meter_assignment_pipeline_payload_sha256, '<null>')",
        "coalesce(history_opening_reading_pipeline_payload_sha256, '<null>')",
        "coalesce(history_closing_reading_pipeline_payload_sha256, '<null>')",
        "coalesce(history_commitment_pipeline_payload_sha256, '<null>')",
        "coalesce(history_excess_order_pipeline_payload_sha256, '<null>')",
        "coalesce(history_capacity_pipeline_payload_sha256, '<null>')",
        "coalesce(cast(history_opening_register_mwh_th as varchar), '<null>')",
        "coalesce(cast(history_closing_register_mwh_th as varchar), '<null>')",
        "coalesce(cast(history_committed_mwh_th as varchar), '<null>')",
        "coalesce(cast(history_delivered_mwh_th as varchar), '<null>')",
        "coalesce(cast(history_shortfall_mwh_th as varchar), '<null>')",
        "coalesce(cast(history_excess_mwh_th as varchar), '<null>')",
        "coalesce(cast(history_deliverable_capacity_mwh_th as varchar), '<null>')",
        "coalesce(cast(history_approved_extra_mwh_th as varchar), '<null>')",
        "coalesce(cast(history_billable_mwh_th as varchar), '<null>')",
        "coalesce(cast(history_gross_earned_revenue_gbp as varchar), '<null>')",
        "coalesce(cast(history_accrued_sla_penalty_gbp as varchar), '<null>')",
        "coalesce(cast(history_net_earned_revenue_gbp as varchar), '<null>')",
        'history_delivery_measurement_status',
        'history_commitment_status',
        'history_capacity_status',
        'history_contract_status',
        'history_customer_access_status',
        'history_correction_status'
    ]) }} as result_state_sha256
from with_status_key
