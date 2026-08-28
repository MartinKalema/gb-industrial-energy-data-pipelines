with expected (
    interval_start_utc,
    committed_mwh_th,
    delivered_mwh_th,
    shortfall_mwh_th,
    approved_extra_mwh_th,
    billable_mwh_th,
    unbilled_excess_mwh_th,
    commitment_status,
    capacity_status,
    deliverable_capacity_mwh_th,
    check_delivery_measures
) as (
    values
        (
            timestamp '2026-08-26 00:00:00 UTC',
            decimal '5.000000', decimal '4.700000', decimal '0.300000',
            decimal '0.000000', decimal '4.700000', decimal '0.000000',
            'committed', 'final', decimal '6.500000', true
        ),
        (
            timestamp '2026-08-26 00:30:00 UTC',
            decimal '5.000000', decimal '5.600000', decimal '0.000000',
            decimal '0.400000', decimal '5.400000', decimal '0.200000',
            'committed', 'final', decimal '6.500000', true
        ),
        (
            timestamp '2026-08-26 01:30:00 UTC',
            decimal '0.000000', decimal '0.000000', decimal '0.000000',
            decimal '0.000000', decimal '0.000000', decimal '0.000000',
            'no_commitment', 'final', decimal '6.500000', true
        ),
        (
            timestamp '2026-08-26 02:00:00 UTC',
            cast(null as decimal(20, 6)), decimal '4.900000',
            cast(null as decimal(20, 6)), decimal '0.000000',
            cast(null as decimal(20, 6)), cast(null as decimal(20, 6)),
            'missing', 'final', decimal '6.500000', true
        ),
        (
            timestamp '2026-08-26 03:00:00 UTC',
            decimal '5.000000', decimal '4.900000', decimal '0.100000',
            decimal '0.000000', decimal '4.900000', decimal '0.000000',
            'committed', 'final', decimal '6.500000', true
        ),
        (
            timestamp '2026-08-26 03:30:00 UTC',
            decimal '5.000000', decimal '5.100000', decimal '0.000000',
            decimal '0.000000', decimal '5.000000', decimal '0.100000',
            'committed', 'final', decimal '6.500000', true
        ),
        (
            timestamp '2026-08-26 04:00:00 UTC',
            decimal '5.500000', decimal '5.200000', decimal '0.300000',
            decimal '0.000000', decimal '5.200000', decimal '0.000000',
            'committed', 'final', decimal '6.500000', true
        ),
        (
            timestamp '2026-08-26 05:00:00 UTC',
            decimal '5.000000', decimal '5.020000', decimal '0.000000',
            decimal '0.000000', decimal '5.000000', decimal '0.020000',
            'committed', 'final', decimal '4.500000', true
        ),
        (
            timestamp '2026-08-26 05:30:00 UTC',
            decimal '5.000000', decimal '5.040000', decimal '0.000000',
            decimal '0.000000', decimal '5.000000', decimal '0.040000',
            'committed', 'provisional', cast(null as decimal(20, 6)), true
        ),
        (
            timestamp '2026-08-26 06:00:00 UTC',
            decimal '5.000000', decimal '5.030000', decimal '0.000000',
            decimal '0.000000', decimal '5.000000', decimal '0.030000',
            'committed', 'final', decimal '0.000000', true
        ),
        (
            timestamp '2026-08-26 06:30:00 UTC',
            decimal '5.000000', decimal '5.010000', decimal '0.000000',
            decimal '0.000000', decimal '5.000000', decimal '0.010000',
            'committed', 'missing', cast(null as decimal(20, 6)), true
        )
),
actual as (
    select *
    from {{ ref('fct_steam_delivery_interval') }}
    where delivery_point_natural_id = 'DP-001'
      and date_key = 20260826
)

select expected.*
from expected
left join actual
  on expected.interval_start_utc = actual.interval_start_utc
where actual.delivery_interval_key is null
   or (
        expected.check_delivery_measures
        and (
            actual.committed_mwh_th is distinct from expected.committed_mwh_th
         or actual.delivered_mwh_th is distinct from expected.delivered_mwh_th
         or actual.shortfall_mwh_th is distinct from expected.shortfall_mwh_th
         or actual.approved_extra_mwh_th is distinct from expected.approved_extra_mwh_th
         or actual.billable_mwh_th is distinct from expected.billable_mwh_th
         or actual.unbilled_excess_mwh_th
                is distinct from expected.unbilled_excess_mwh_th
        )
      )
   or actual.commitment_status is distinct from expected.commitment_status
   or actual.capacity_status is distinct from expected.capacity_status
   or actual.deliverable_capacity_mwh_th
        is distinct from expected.deliverable_capacity_mwh_th
