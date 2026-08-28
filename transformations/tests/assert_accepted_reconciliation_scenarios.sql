with scenario_intervals (
    scenario_id,
    scenario_state,
    interval_number,
    committed_mwh_th,
    delivered_mwh_th,
    deliverable_capacity_mwh_th
) as (
    values
        ('DM-016', 'final', 1, decimal '5.0', decimal '5.2', decimal '5.5'),
        ('DM-016', 'final', 2, decimal '5.0', decimal '4.8', decimal '4.8'),
        ('DM-017', 'initial', 1, decimal '5.0', decimal '5.0', decimal '5.0'),
        ('DM-017', 'initial', 2, decimal '5.0', cast(null as decimal(2, 1)), decimal '5.0'),
        ('DM-017', 'reconciled', 1, decimal '5.0', decimal '5.0', decimal '5.0'),
        ('DM-017', 'reconciled', 2, decimal '5.0', decimal '4.5', decimal '5.0'),
        ('DM-018', 'original', 1, decimal '5.0', decimal '4.7', decimal '5.0'),
        ('DM-018', 'original', 2, decimal '5.0', decimal '5.3', decimal '5.0'),
        ('DM-018', 'corrected', 1, decimal '5.0', decimal '4.9', decimal '5.0'),
        ('DM-018', 'corrected', 2, decimal '5.0', decimal '5.1', decimal '5.0')
),

fixture_inputs as (
    select
        scenario_intervals.*,
        'fixture-commitment' as source_commitment_revision_id,
        'active' as schedule_record_status,
        'committed' as obligation_status,
        'final' as capacity_state,
        cast(null as bigint) as eligible_order_count,
        cast(null as varchar) as excess_order_state,
        cast(null as decimal(20, 6)) as source_approved_extra_mwh_th,
        'fixture-customer-version' as customer_version_id,
        'fixture-site-version' as site_version_id,
        'fixture-contract-version' as contract_terms_version_id,
        cast(decimal '50.000000' as decimal(18, 6))
            as energy_rate_gbp_per_mwh_th,
        cast(decimal '100.000000' as decimal(18, 6))
            as sla_penalty_rate_gbp_per_mwh_th,
        'accepted' as delivery_measurement_status,
        cast(1 as bigint) as delivery_point_assignment_source_revision,
        cast(1 as bigint) as customer_source_revision,
        cast(1 as bigint) as site_source_revision,
        cast(1 as bigint) as contract_source_revision,
        cast(1 as bigint) as meter_assignment_source_revision,
        cast(1 as bigint) as opening_reading_source_revision,
        cast(1 as bigint) as closing_reading_source_revision,
        cast(1 as bigint) as commitment_source_revision,
        cast(null as bigint) as excess_order_source_revision,
        cast(1 as bigint) as capacity_source_revision
    from scenario_intervals
),

calculated_intervals as (
    {{ steam_delivery_interval_calculations('fixture_inputs') }}
),

actual as (
    select
        scenario_id,
        scenario_state,
        sum(committed_mwh_th) as committed_mwh_th,
        sum(delivered_mwh_th) as known_delivered_mwh_th,
        count(delivered_mwh_th) as accepted_delivery_count,
        sum(shortfall_mwh_th) as known_shortfall_mwh_th,
        sum(excess_mwh_th) as known_excess_mwh_th,
        sum(billable_mwh_th) as known_billable_mwh_th,
        case when count(delivered_mwh_th) = count(*) then cast(
            sum(sla_attainment_numerator_mwh_th) / sum(committed_mwh_th) * 100
            as decimal(20, 6)
        ) end as final_sla_pct,
        cast(
            sum(contractual_availability_numerator_mwh_th)
                / sum(committed_mwh_th) * 100
            as decimal(20, 6)
        ) as availability_pct,
        sum(gross_earned_revenue_gbp) as known_gross_gbp,
        case when count(delivered_mwh_th) = count(*)
            then sum(accrued_sla_penalty_gbp) end as final_penalty_gbp,
        case when count(delivered_mwh_th) = count(*)
            then sum(net_earned_revenue_gbp) end as final_net_gbp
    from calculated_intervals
    group by scenario_id, scenario_state
),

expected (
    scenario_id,
    scenario_state,
    committed_mwh_th,
    known_delivered_mwh_th,
    accepted_delivery_count,
    known_shortfall_mwh_th,
    known_excess_mwh_th,
    known_billable_mwh_th,
    final_sla_pct,
    availability_pct,
    known_gross_gbp,
    final_penalty_gbp,
    final_net_gbp
) as (
    values
        ('DM-016', 'final', decimal '10.0', decimal '10.0', 2,
         decimal '0.2', decimal '0.2', decimal '9.8', decimal '98.000000',
         decimal '98.000000', decimal '490.000000000000',
         decimal '20.000000000000', decimal '470.000000000000'),
        ('DM-017', 'initial', decimal '10.0', decimal '5.0', 1,
         decimal '0.0', decimal '0.0', decimal '5.0',
         cast(null as decimal(20, 6)), decimal '100.000000',
         decimal '250.000000000000', cast(null as decimal(38, 12)),
         cast(null as decimal(38, 12))),
        ('DM-017', 'reconciled', decimal '10.0', decimal '9.5', 2,
         decimal '0.5', decimal '0.0', decimal '9.5', decimal '95.000000',
         decimal '100.000000', decimal '475.000000000000',
         decimal '50.000000000000', decimal '425.000000000000'),
        ('DM-018', 'original', decimal '10.0', decimal '10.0', 2,
         decimal '0.3', decimal '0.3', decimal '9.7', decimal '97.000000',
         decimal '100.000000', decimal '485.000000000000',
         decimal '30.000000000000', decimal '455.000000000000'),
        ('DM-018', 'corrected', decimal '10.0', decimal '10.0', 2,
         decimal '0.1', decimal '0.1', decimal '9.9', decimal '99.000000',
         decimal '100.000000', decimal '495.000000000000',
         decimal '10.000000000000', decimal '485.000000000000')
)

select expected.*
from expected
left join actual
  on expected.scenario_id = actual.scenario_id
 and expected.scenario_state = actual.scenario_state
where actual.scenario_id is null
   or actual.committed_mwh_th is distinct from expected.committed_mwh_th
   or actual.known_delivered_mwh_th is distinct from expected.known_delivered_mwh_th
   or actual.accepted_delivery_count is distinct from expected.accepted_delivery_count
   or actual.known_shortfall_mwh_th is distinct from expected.known_shortfall_mwh_th
   or actual.known_excess_mwh_th is distinct from expected.known_excess_mwh_th
   or actual.known_billable_mwh_th is distinct from expected.known_billable_mwh_th
   or actual.final_sla_pct is distinct from expected.final_sla_pct
   or actual.availability_pct is distinct from expected.availability_pct
   or actual.known_gross_gbp is distinct from expected.known_gross_gbp
   or actual.final_penalty_gbp is distinct from expected.final_penalty_gbp
   or actual.final_net_gbp is distinct from expected.final_net_gbp
