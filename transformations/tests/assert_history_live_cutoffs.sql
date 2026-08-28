with retro_expected (
    phase, as_of_utc, committed_mwh_th, delivered_mwh_th, shortfall_mwh_th,
    billable_mwh_th, gross_gbp, penalty_gbp, net_gbp
) as (
    values
        ('before', timestamp '2026-08-26 10:29:59.999999 UTC',
         decimal '5.000000', decimal '5.200000', decimal '0.000000',
         decimal '5.000000', decimal '250.000000000000',
         decimal '0.000000000000', decimal '250.000000000000'),
        ('after', timestamp '2026-08-26 10:30:00 UTC',
         decimal '5.500000', decimal '5.200000', decimal '0.300000',
         decimal '5.200000', decimal '260.000000000000',
         decimal '30.000000000000', decimal '230.000000000000')
),
retro_mismatches as (
    select 'retro_commitment_' || expected.phase as failure
    from retro_expected as expected
    left join {{ ref('fct_steam_delivery_interval_history') }} as actual
      on actual.delivery_point_natural_id = 'DP-001'
     and actual.interval_start_utc = timestamp '2026-08-26 04:00:00 UTC'
     and actual.known_from_utc <= expected.as_of_utc
     and (actual.known_to_utc is null or expected.as_of_utc < actual.known_to_utc)
    where actual.delivery_interval_history_key is null
       or actual.committed_mwh_th is distinct from expected.committed_mwh_th
       or actual.delivered_mwh_th is distinct from expected.delivered_mwh_th
       or actual.shortfall_mwh_th is distinct from expected.shortfall_mwh_th
       or actual.billable_mwh_th is distinct from expected.billable_mwh_th
       or actual.gross_earned_revenue_gbp is distinct from expected.gross_gbp
       or actual.accrued_sla_penalty_gbp is distinct from expected.penalty_gbp
       or actual.net_earned_revenue_gbp is distinct from expected.net_gbp
       or (expected.phase = 'after' and actual.known_from_utc
            is distinct from timestamp '2026-08-26 10:30:00 UTC')
),

boundary_expected (
    phase, as_of_utc, interval_start_utc, delivered_mwh_th,
    shortfall_mwh_th, excess_mwh_th, billable_mwh_th,
    unbilled_excess_mwh_th, gross_gbp, penalty_gbp, net_gbp
) as (
    values
        ('before', timestamp '2026-08-27 09:29:59.999999 UTC',
         timestamp '2026-08-26 03:00:00 UTC', decimal '4.700000',
         decimal '0.300000', decimal '0.000000', decimal '4.700000',
         decimal '0.000000', decimal '235.000000000000',
         decimal '30.000000000000', decimal '205.000000000000'),
        ('before', timestamp '2026-08-27 09:29:59.999999 UTC',
         timestamp '2026-08-26 03:30:00 UTC', decimal '5.300000',
         decimal '0.000000', decimal '0.300000', decimal '5.000000',
         decimal '0.300000', decimal '250.000000000000',
         decimal '0.000000000000', decimal '250.000000000000'),
        ('after', timestamp '2026-08-27 09:30:00 UTC',
         timestamp '2026-08-26 03:00:00 UTC', decimal '4.900000',
         decimal '0.100000', decimal '0.000000', decimal '4.900000',
         decimal '0.000000', decimal '245.000000000000',
         decimal '10.000000000000', decimal '235.000000000000'),
        ('after', timestamp '2026-08-27 09:30:00 UTC',
         timestamp '2026-08-26 03:30:00 UTC', decimal '5.100000',
         decimal '0.000000', decimal '0.100000', decimal '5.000000',
         decimal '0.100000', decimal '250.000000000000',
         decimal '0.000000000000', decimal '250.000000000000')
),
boundary_mismatches as (
    select 'shared_boundary_' || expected.phase || '_'
        || cast(expected.interval_start_utc as varchar) as failure
    from boundary_expected as expected
    left join {{ ref('fct_steam_delivery_interval_history') }} as actual
      on actual.delivery_point_natural_id = 'DP-001'
     and actual.interval_start_utc = expected.interval_start_utc
     and actual.known_from_utc <= expected.as_of_utc
     and (actual.known_to_utc is null or expected.as_of_utc < actual.known_to_utc)
    where actual.delivery_interval_history_key is null
       or actual.delivered_mwh_th is distinct from expected.delivered_mwh_th
       or actual.shortfall_mwh_th is distinct from expected.shortfall_mwh_th
       or actual.excess_mwh_th is distinct from expected.excess_mwh_th
       or actual.billable_mwh_th is distinct from expected.billable_mwh_th
       or actual.unbilled_excess_mwh_th
            is distinct from expected.unbilled_excess_mwh_th
       or actual.gross_earned_revenue_gbp is distinct from expected.gross_gbp
       or actual.accrued_sla_penalty_gbp is distinct from expected.penalty_gbp
       or actual.net_earned_revenue_gbp is distinct from expected.net_gbp
       or (expected.phase = 'after' and actual.known_from_utc
            is distinct from timestamp '2026-08-27 09:30:00 UTC')
),

boundary_aggregate_expected (
    phase, as_of_utc, interval_count, committed_mwh_th, delivered_mwh_th,
    shortfall_mwh_th, excess_mwh_th, billable_mwh_th,
    sla_numerator_mwh_th, sla_pct, availability_pct,
    gross_gbp, penalty_gbp, net_gbp
) as (
    values
        ('before', timestamp '2026-08-27 09:29:59.999999 UTC', 2,
         decimal '10.000000', decimal '10.000000', decimal '0.300000',
         decimal '0.300000', decimal '9.700000', decimal '9.700000',
         decimal '97.000000', decimal '100.000000',
         decimal '485.000000000000', decimal '30.000000000000',
         decimal '455.000000000000'),
        ('after', timestamp '2026-08-27 09:30:00 UTC', 2,
         decimal '10.000000', decimal '10.000000', decimal '0.100000',
         decimal '0.100000', decimal '9.900000', decimal '9.900000',
         decimal '99.000000', decimal '100.000000',
         decimal '495.000000000000', decimal '10.000000000000',
         decimal '485.000000000000')
),
boundary_aggregate_actual as (
    select
        expected.phase,
        count(history.delivery_interval_history_key) as interval_count,
        sum(history.committed_mwh_th) as committed_mwh_th,
        sum(history.delivered_mwh_th) as delivered_mwh_th,
        sum(history.shortfall_mwh_th) as shortfall_mwh_th,
        sum(history.excess_mwh_th) as excess_mwh_th,
        sum(history.billable_mwh_th) as billable_mwh_th,
        sum(history.sla_attainment_numerator_mwh_th) as sla_numerator_mwh_th,
        cast(
            sum(history.sla_attainment_numerator_mwh_th)
                / sum(history.committed_mwh_th) * 100
            as decimal(20, 6)
        ) as sla_pct,
        cast(
            sum(history.contractual_availability_numerator_mwh_th)
                / sum(history.committed_mwh_th) * 100
            as decimal(20, 6)
        ) as availability_pct,
        sum(history.gross_earned_revenue_gbp) as gross_gbp,
        sum(history.accrued_sla_penalty_gbp) as penalty_gbp,
        sum(history.net_earned_revenue_gbp) as net_gbp
    from boundary_aggregate_expected as expected
    left join {{ ref('fct_steam_delivery_interval_history') }} as history
      on history.delivery_point_natural_id = 'DP-001'
     and history.interval_start_utc in (
            timestamp '2026-08-26 03:00:00 UTC',
            timestamp '2026-08-26 03:30:00 UTC'
         )
     and history.known_from_utc <= expected.as_of_utc
     and (history.known_to_utc is null or expected.as_of_utc < history.known_to_utc)
    group by expected.phase
),
boundary_aggregate_mismatches as (
    select 'shared_boundary_aggregate_' || expected.phase as failure
    from boundary_aggregate_expected as expected
    left join boundary_aggregate_actual as actual
      on expected.phase = actual.phase
    where actual.phase is null
       or actual.interval_count is distinct from expected.interval_count
       or actual.committed_mwh_th is distinct from expected.committed_mwh_th
       or actual.delivered_mwh_th is distinct from expected.delivered_mwh_th
       or actual.shortfall_mwh_th is distinct from expected.shortfall_mwh_th
       or actual.excess_mwh_th is distinct from expected.excess_mwh_th
       or actual.billable_mwh_th is distinct from expected.billable_mwh_th
       or actual.sla_numerator_mwh_th is distinct from expected.sla_numerator_mwh_th
       or actual.sla_pct is distinct from expected.sla_pct
       or actual.availability_pct is distinct from expected.availability_pct
       or actual.gross_gbp is distinct from expected.gross_gbp
       or actual.penalty_gbp is distinct from expected.penalty_gbp
       or actual.net_gbp is distinct from expected.net_gbp
),

capacity_expected (
    phase, as_of_utc, capacity_status, deliverable_capacity_mwh_th,
    availability_numerator_mwh_th
) as (
    values
        ('missing', timestamp '2026-08-26 03:59:59.999999 UTC', 'missing',
         cast(null as decimal(20, 6)), cast(null as decimal(20, 6))),
        ('provisional', timestamp '2026-08-26 04:00:00 UTC', 'provisional',
         cast(null as decimal(20, 6)), cast(null as decimal(20, 6))),
        ('final', timestamp '2026-08-26 07:30:00 UTC', 'final',
         decimal '5.500000', decimal '5.000000'),
        ('corrected', timestamp '2026-08-27 05:30:00 UTC', 'final',
         decimal '4.500000', decimal '4.500000')
),
capacity_mismatches as (
    select 'capacity_' || expected.phase as failure
    from capacity_expected as expected
    left join {{ ref('fct_steam_delivery_interval_history') }} as actual
      on actual.delivery_point_natural_id = 'DP-001'
     and actual.interval_start_utc = timestamp '2026-08-26 05:00:00 UTC'
     and actual.known_from_utc <= expected.as_of_utc
     and (actual.known_to_utc is null or expected.as_of_utc < actual.known_to_utc)
    where actual.delivery_interval_history_key is null
       or actual.capacity_status is distinct from expected.capacity_status
       or actual.deliverable_capacity_mwh_th
            is distinct from expected.deliverable_capacity_mwh_th
       or actual.contractual_availability_numerator_mwh_th
            is distinct from expected.availability_numerator_mwh_th
       or (expected.phase <> 'missing' and actual.known_from_utc
            is distinct from expected.as_of_utc)
),

lineage_mismatches as (
    select 'customer_revision_before' as failure
    where not exists (
        select 1
        from {{ ref('fct_steam_delivery_interval_history') }} as history
        inner join {{ ref('dim_customer_revision_audit') }} as audit
          on history.customer_revision_key = audit.customer_revision_key
        where history.delivery_point_natural_id = 'DP-001'
          and history.interval_start_utc = timestamp '2026-08-26 03:00:00 UTC'
          and history.known_from_utc <= timestamp '2026-08-05 22:59:59.999999 UTC'
          and (history.known_to_utc is null
               or timestamp '2026-08-05 22:59:59.999999 UTC' < history.known_to_utc)
          and history.customer_source_revision = 1
          and audit.legal_name = 'Northstar Advanced Ceramcis Ltd'
    )

    union all

    select 'customer_revision_after' as failure
    where not exists (
        select 1
        from {{ ref('fct_steam_delivery_interval_history') }} as history
        inner join {{ ref('dim_customer_revision_audit') }} as audit
          on history.customer_revision_key = audit.customer_revision_key
        where history.delivery_point_natural_id = 'DP-001'
          and history.interval_start_utc = timestamp '2026-08-26 03:00:00 UTC'
          and history.known_from_utc = timestamp '2026-08-05 23:00:00 UTC'
          and history.customer_source_revision = 2
          and audit.legal_name = 'Northstar Advanced Ceramics Ltd'
    )

    union all

    select 'site_revision_before' as failure
    where not exists (
        select 1
        from {{ ref('fct_steam_delivery_interval_history') }} as history
        inner join {{ ref('dim_site_revision_audit') }} as audit
          on history.site_revision_key = audit.site_revision_key
        where history.delivery_point_natural_id = 'DP-001'
          and history.interval_start_utc = timestamp '2026-08-26 03:00:00 UTC'
          and history.known_from_utc <= timestamp '2026-08-07 22:59:59.999999 UTC'
          and (history.known_to_utc is null
               or timestamp '2026-08-07 22:59:59.999999 UTC' < history.known_to_utc)
          and history.site_source_revision = 1
          and audit.locality = 'Sheffeld'
    )

    union all

    select 'site_revision_after' as failure
    where not exists (
        select 1
        from {{ ref('fct_steam_delivery_interval_history') }} as history
        inner join {{ ref('dim_site_revision_audit') }} as audit
          on history.site_revision_key = audit.site_revision_key
        where history.delivery_point_natural_id = 'DP-001'
          and history.interval_start_utc = timestamp '2026-08-26 03:00:00 UTC'
          and history.known_from_utc = timestamp '2026-08-07 23:00:00 UTC'
          and history.site_source_revision = 2
          and audit.locality = 'Sheffield'
    )

    union all

    select 'contract_revision_before' as failure
    where not exists (
        select 1
        from {{ ref('fct_steam_delivery_interval_history') }} as history
        inner join {{ ref('dim_contract_revision_audit') }} as audit
          on history.contract_revision_key = audit.contract_revision_key
        where history.delivery_point_natural_id = 'DP-001'
          and history.interval_start_utc = timestamp '2026-08-26 03:00:00 UTC'
          and history.known_from_utc <= timestamp '2026-08-15 22:59:59.999999 UTC'
          and (history.known_to_utc is null
               or timestamp '2026-08-15 22:59:59.999999 UTC' < history.known_to_utc)
          and history.contract_source_revision = 1
          and audit.energy_rate_gbp_per_mwh_th = decimal '49.500000'
    )

    union all

    select 'contract_revision_after' as failure
    where not exists (
        select 1
        from {{ ref('fct_steam_delivery_interval_history') }} as history
        inner join {{ ref('dim_contract_revision_audit') }} as audit
          on history.contract_revision_key = audit.contract_revision_key
        where history.delivery_point_natural_id = 'DP-001'
          and history.interval_start_utc = timestamp '2026-08-26 03:00:00 UTC'
          and history.known_from_utc = timestamp '2026-08-15 23:00:00 UTC'
          and history.contract_source_revision = 2
          and history.energy_rate_gbp_per_mwh_th = decimal '50.000000'
          and audit.energy_rate_gbp_per_mwh_th = decimal '50.000000'
    )
)

select * from retro_mismatches
union all select * from boundary_mismatches
union all select * from boundary_aggregate_mismatches
union all select * from capacity_mismatches
union all select * from lineage_mismatches
