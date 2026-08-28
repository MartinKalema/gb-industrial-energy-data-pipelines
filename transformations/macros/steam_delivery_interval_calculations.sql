{% macro steam_delivery_interval_calculations(input_relation) -%}
with normalized_inputs as (
    select
        inputs.*,
        case
            when source_commitment_revision_id is null then 'missing'
            when schedule_record_status = 'withdrawn' then 'withdrawn'
            else obligation_status
        end as commitment_status,
        coalesce(capacity_state, 'missing') as normalized_capacity_status,
        case
            when eligible_order_count is null
                then cast(decimal '0.000000' as decimal(20, 6))
            when eligible_order_count = 1 and excess_order_state = 'approved'
                then source_approved_extra_mwh_th
            when eligible_order_count = 1 and excess_order_state = 'cancelled'
                then cast(decimal '0.000000' as decimal(20, 6))
        end as approved_extra_mwh_th,
        case
            when customer_version_id is not null
             and site_version_id is not null then 'authorized'
            else 'denied'
        end as customer_access_status,
        case when contract_terms_version_id is not null then 'accepted' else 'missing' end
            as contract_status
    from {{ input_relation }} as inputs
),

physical_measures as (
    select
        normalized_inputs.*,
        cast(null as decimal(20, 6)) as delivered_steam_t,
        case
            when delivered_mwh_th is not null
             and committed_mwh_th is not null
                then cast(
                    greatest(committed_mwh_th - delivered_mwh_th, decimal '0.000000')
                    as decimal(20, 6)
                )
        end as shortfall_mwh_th,
        case
            when delivered_mwh_th is not null
             and committed_mwh_th is not null
                then cast(
                    greatest(delivered_mwh_th - committed_mwh_th, decimal '0.000000')
                    as decimal(20, 6)
                )
        end as excess_mwh_th,
        case
            when delivered_mwh_th is not null
             and committed_mwh_th is not null
             and contract_terms_version_id is not null
             and approved_extra_mwh_th is not null
                then cast(
                    least(delivered_mwh_th, committed_mwh_th + approved_extra_mwh_th)
                    as decimal(20, 6)
                )
        end as billable_mwh_th,
        case
            when delivered_mwh_th is not null
             and committed_mwh_th is not null
             and approved_extra_mwh_th is not null
                then cast(
                    greatest(
                        delivered_mwh_th - committed_mwh_th - approved_extra_mwh_th,
                        decimal '0.000000'
                    ) as decimal(20, 6)
                )
        end as unbilled_excess_mwh_th
    from normalized_inputs
),

financial_and_metric_inputs as (
    select
        physical_measures.*,
        case
            when billable_mwh_th is not null
             and energy_rate_gbp_per_mwh_th is not null
                then cast(
                    billable_mwh_th * energy_rate_gbp_per_mwh_th
                    as decimal(38, 12)
                )
        end as gross_earned_revenue_gbp,
        case
            when shortfall_mwh_th is not null
             and sla_penalty_rate_gbp_per_mwh_th is not null
                then cast(
                    shortfall_mwh_th * sla_penalty_rate_gbp_per_mwh_th
                    as decimal(38, 12)
                )
        end as accrued_sla_penalty_gbp,
        case
            when committed_mwh_th is null then cast(null as decimal(20, 6))
            when committed_mwh_th = decimal '0.000000'
                then cast(decimal '0.000000' as decimal(20, 6))
            when delivered_mwh_th is not null
                then cast(least(delivered_mwh_th, committed_mwh_th) as decimal(20, 6))
        end as sla_attainment_numerator_mwh_th,
        case
            when committed_mwh_th is null then cast(null as decimal(20, 6))
            when committed_mwh_th = decimal '0.000000'
                then cast(decimal '0.000000' as decimal(20, 6))
            when normalized_capacity_status = 'final'
             and deliverable_capacity_mwh_th is not null
                then cast(
                    least(deliverable_capacity_mwh_th, committed_mwh_th)
                    as decimal(20, 6)
                )
        end as contractual_availability_numerator_mwh_th,
        cast(1 as bigint) as expected_interval_count,
        case when source_commitment_revision_id is not null
            then cast(1 as bigint) else cast(0 as bigint) end
            as commitment_record_count,
        case
            when committed_mwh_th > decimal '0.000000' then cast(1 as bigint)
            when committed_mwh_th = decimal '0.000000' then cast(0 as bigint)
        end as applicable_interval_count,
        case
            when committed_mwh_th > decimal '0.000000'
             and delivered_mwh_th is not null then cast(1 as bigint)
            when committed_mwh_th > decimal '0.000000' then cast(0 as bigint)
            when committed_mwh_th = decimal '0.000000' then cast(0 as bigint)
        end as accepted_applicable_delivery_count,
        case
            when committed_mwh_th > decimal '0.000000'
             and normalized_capacity_status = 'final' then cast(1 as bigint)
            when committed_mwh_th > decimal '0.000000' then cast(0 as bigint)
            when committed_mwh_th = decimal '0.000000' then cast(0 as bigint)
        end as final_applicable_capacity_count
    from physical_measures
),

completed as (
    select
        financial_and_metric_inputs.*,
        case
            when gross_earned_revenue_gbp is not null
             and accrued_sla_penalty_gbp is not null
                then cast(
                    gross_earned_revenue_gbp - accrued_sla_penalty_gbp
                    as decimal(38, 12)
                )
        end as net_earned_revenue_gbp,
        case
            when commitment_status = 'no_commitment' then 'not_applicable'
            when commitment_status in ('missing', 'withdrawn')
              or delivery_measurement_status <> 'accepted' then 'provisional'
            else 'final'
        end as sla_result_status,
        case
            when commitment_status = 'no_commitment' then 'not_applicable'
            when commitment_status in ('missing', 'withdrawn')
              or normalized_capacity_status <> 'final' then 'provisional'
            else 'final'
        end as availability_result_status,
        case
            when commitment_status in ('missing', 'withdrawn')
              or delivery_measurement_status <> 'accepted'
              or contract_status <> 'accepted'
              or approved_extra_mwh_th is null then 'provisional'
            else 'final'
        end as financial_result_status,
        case
            when coalesce(delivery_point_assignment_source_revision, 1) > 1
              or coalesce(customer_source_revision, 1) > 1
              or coalesce(site_source_revision, 1) > 1
              or coalesce(contract_source_revision, 1) > 1
              or coalesce(meter_assignment_source_revision, 1) > 1
              or coalesce(opening_reading_source_revision, 1) > 1
              or coalesce(closing_reading_source_revision, 1) > 1
              or coalesce(commitment_source_revision, 1) > 1
              or coalesce(excess_order_source_revision, 1) > 1
              or coalesce(capacity_source_revision, 1) > 1
                then 'corrected'
            else 'original'
        end as correction_status
    from financial_and_metric_inputs
)

select *
from completed
{%- endmacro %}
