with terminal_history_counts as (
    select
        delivery_interval_key,
        count(*) as terminal_row_count
    from {{ ref('fct_steam_delivery_interval_history') }}
    where known_to_utc is null
    group by delivery_interval_key
),

current_without_one_terminal as (
    select current_fact.delivery_interval_key
    from {{ ref('fct_steam_delivery_interval') }} as current_fact
    left join terminal_history_counts
      on current_fact.delivery_interval_key = terminal_history_counts.delivery_interval_key
    where coalesce(terminal_history_counts.terminal_row_count, 0) <> 1
),

history_without_one_terminal as (
    select delivery_interval_key
    from {{ ref('fct_steam_delivery_interval_history') }}
    group by delivery_interval_key
    having count_if(known_to_utc is null) <> 1
),

unexpected_active_history_without_current as (
    select history_fact.delivery_interval_key
    from {{ ref('fct_steam_delivery_interval_history') }} as history_fact
    left join {{ ref('fct_steam_delivery_interval') }} as current_fact
      on history_fact.delivery_interval_key = current_fact.delivery_interval_key
    where history_fact.known_to_utc is null
      and current_fact.delivery_interval_key is null
      and history_fact.delivery_point_assignment_id is not null
),

parity_mismatches as (
    select current_fact.delivery_interval_key
    from {{ ref('fct_steam_delivery_interval') }} as current_fact
    inner join {{ ref('fct_steam_delivery_interval_history') }} as history_fact
      on current_fact.delivery_interval_key = history_fact.delivery_interval_key
     and history_fact.known_to_utc is null
    where current_fact.committed_mwh_th is distinct from history_fact.committed_mwh_th
       or current_fact.delivered_mwh_th is distinct from history_fact.delivered_mwh_th
       or current_fact.shortfall_mwh_th is distinct from history_fact.shortfall_mwh_th
       or current_fact.excess_mwh_th is distinct from history_fact.excess_mwh_th
       or current_fact.deliverable_capacity_mwh_th
            is distinct from history_fact.deliverable_capacity_mwh_th
       or current_fact.approved_extra_mwh_th
            is distinct from history_fact.approved_extra_mwh_th
       or current_fact.billable_mwh_th is distinct from history_fact.billable_mwh_th
       or current_fact.unbilled_excess_mwh_th
            is distinct from history_fact.unbilled_excess_mwh_th
       or current_fact.gross_earned_revenue_gbp
            is distinct from history_fact.gross_earned_revenue_gbp
       or current_fact.accrued_sla_penalty_gbp
            is distinct from history_fact.accrued_sla_penalty_gbp
       or current_fact.net_earned_revenue_gbp
            is distinct from history_fact.net_earned_revenue_gbp
       or current_fact.sla_attainment_numerator_mwh_th
            is distinct from history_fact.sla_attainment_numerator_mwh_th
       or current_fact.contractual_availability_numerator_mwh_th
            is distinct from history_fact.contractual_availability_numerator_mwh_th
       or current_fact.expected_interval_count
            is distinct from history_fact.expected_interval_count
       or current_fact.commitment_record_count
            is distinct from history_fact.commitment_record_count
       or current_fact.applicable_interval_count
            is distinct from history_fact.applicable_interval_count
       or current_fact.accepted_applicable_delivery_count
            is distinct from history_fact.accepted_applicable_delivery_count
       or current_fact.final_applicable_capacity_count
            is distinct from history_fact.final_applicable_capacity_count
       or current_fact.energy_rate_gbp_per_mwh_th
            is distinct from history_fact.energy_rate_gbp_per_mwh_th
       or current_fact.sla_penalty_rate_gbp_per_mwh_th
            is distinct from history_fact.sla_penalty_rate_gbp_per_mwh_th
       or current_fact.delivery_measurement_status
            is distinct from history_fact.delivery_measurement_status
       or current_fact.commitment_status is distinct from history_fact.commitment_status
       or current_fact.capacity_status is distinct from history_fact.capacity_status
       or current_fact.contract_status is distinct from history_fact.contract_status
       or current_fact.customer_access_status
            is distinct from history_fact.customer_access_status
       or current_fact.sla_result_status is distinct from history_fact.sla_result_status
       or current_fact.availability_result_status
            is distinct from history_fact.availability_result_status
       or current_fact.financial_result_status
            is distinct from history_fact.financial_result_status
       or current_fact.correction_status is distinct from history_fact.correction_status
       or current_fact.customer_source_revision
            is distinct from history_fact.customer_source_revision
       or current_fact.customer_pipeline_payload_sha256
            is distinct from history_fact.customer_pipeline_payload_sha256
       or current_fact.site_source_revision is distinct from history_fact.site_source_revision
       or current_fact.site_pipeline_payload_sha256
            is distinct from history_fact.site_pipeline_payload_sha256
       or current_fact.delivery_point_assignment_source_revision
            is distinct from history_fact.delivery_point_assignment_source_revision
       or current_fact.delivery_point_assignment_pipeline_payload_sha256
            is distinct from history_fact.delivery_point_assignment_pipeline_payload_sha256
       or current_fact.contract_source_revision
            is distinct from history_fact.contract_source_revision
       or current_fact.contract_pipeline_payload_sha256
            is distinct from history_fact.contract_pipeline_payload_sha256
       or current_fact.meter_assignment_source_revision
            is distinct from history_fact.meter_assignment_source_revision
       or current_fact.meter_assignment_pipeline_payload_sha256
            is distinct from history_fact.meter_assignment_pipeline_payload_sha256
       or current_fact.commitment_source_revision
            is distinct from history_fact.commitment_source_revision
       or current_fact.commitment_pipeline_payload_sha256
            is distinct from history_fact.commitment_pipeline_payload_sha256
       or current_fact.excess_order_source_revision
            is distinct from history_fact.excess_order_source_revision
       or current_fact.excess_order_pipeline_payload_sha256
            is distinct from history_fact.excess_order_pipeline_payload_sha256
       or current_fact.opening_reading_source_revision
            is distinct from history_fact.opening_reading_source_revision
       or current_fact.opening_reading_pipeline_payload_sha256
            is distinct from history_fact.opening_reading_pipeline_payload_sha256
       or current_fact.closing_reading_source_revision
            is distinct from history_fact.closing_reading_source_revision
       or current_fact.closing_reading_pipeline_payload_sha256
            is distinct from history_fact.closing_reading_pipeline_payload_sha256
       or current_fact.capacity_source_revision
            is distinct from history_fact.capacity_source_revision
       or current_fact.capacity_pipeline_payload_sha256
            is distinct from history_fact.capacity_pipeline_payload_sha256
)

select * from current_without_one_terminal
union all
select * from history_without_one_terminal
union all
select * from unexpected_active_history_without_current
union all
select * from parity_mismatches
