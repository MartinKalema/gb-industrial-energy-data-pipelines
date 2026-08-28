with canonical_inputs as (
    select
        history_inputs.*,
        history_source_commitment_revision_id as source_commitment_revision_id,
        history_schedule_record_status as schedule_record_status,
        history_obligation_status as obligation_status,
        history_capacity_status as capacity_state,
        case when history_eligible_order_state_count = 0 then cast(null as bigint)
            else history_eligible_order_state_count end as eligible_order_count,
        history_order_state as excess_order_state,
        history_approved_extra_mwh_th as source_approved_extra_mwh_th,
        history_customer_version_id as customer_version_id,
        history_site_version_id as site_version_id,
        history_contract_terms_version_id as contract_terms_version_id,
        history_delivered_mwh_th as delivered_mwh_th,
        history_committed_mwh_th as committed_mwh_th,
        history_energy_rate_gbp_per_mwh_th as energy_rate_gbp_per_mwh_th,
        history_sla_penalty_rate_gbp_per_mwh_th as sla_penalty_rate_gbp_per_mwh_th,
        history_deliverable_capacity_mwh_th as deliverable_capacity_mwh_th,
        history_delivery_measurement_status as delivery_measurement_status,
        history_delivery_point_assignment_source_revision
            as delivery_point_assignment_source_revision,
        history_customer_source_revision as customer_source_revision,
        history_site_source_revision as site_source_revision,
        history_contract_source_revision as contract_source_revision,
        history_meter_assignment_source_revision as meter_assignment_source_revision,
        history_opening_reading_source_revision as opening_reading_source_revision,
        history_closing_reading_source_revision as closing_reading_source_revision,
        history_commitment_source_revision as commitment_source_revision,
        history_excess_order_source_revision as excess_order_source_revision,
        history_capacity_source_revision as capacity_source_revision
    from {{ ref('int_delivery_interval_history_inputs') }} as history_inputs
),

calculated as (
    {{ steam_delivery_interval_calculations('canonical_inputs') }}
)

select
    calculated.*,
    commitment_status as history_commitment_status,
    contract_status as history_contract_status,
    customer_access_status as history_customer_access_status,
    delivered_steam_t as history_delivered_steam_t,
    shortfall_mwh_th as history_shortfall_mwh_th,
    excess_mwh_th as history_excess_mwh_th,
    billable_mwh_th as history_billable_mwh_th,
    unbilled_excess_mwh_th as history_unbilled_excess_mwh_th,
    gross_earned_revenue_gbp as history_gross_earned_revenue_gbp,
    accrued_sla_penalty_gbp as history_accrued_sla_penalty_gbp,
    net_earned_revenue_gbp as history_net_earned_revenue_gbp,
    sla_attainment_numerator_mwh_th as history_sla_attainment_numerator_mwh_th,
    contractual_availability_numerator_mwh_th
        as history_contractual_availability_numerator_mwh_th,
    expected_interval_count as history_expected_interval_count,
    commitment_record_count as history_commitment_record_count,
    applicable_interval_count as history_applicable_interval_count,
    accepted_applicable_delivery_count as history_accepted_applicable_delivery_count,
    final_applicable_capacity_count as history_final_applicable_capacity_count,
    sla_result_status as history_sla_result_status,
    availability_result_status as history_availability_result_status,
    financial_result_status as history_financial_result_status,
    correction_status as history_correction_status
from calculated
