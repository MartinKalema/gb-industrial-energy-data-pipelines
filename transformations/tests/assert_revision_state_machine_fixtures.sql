with revision_fixture (
    source_system_id,
    logical_id,
    source_revision,
    record_status,
    published_at_utc,
    approved_at_utc,
    approval_state,
    pipeline_identity_sha256
) as (
    values
        ('fixture', 'late-lower', 1, 'active',
         timestamp '2026-01-01 00:00:00 UTC', timestamp '2026-01-01 00:00:00 UTC',
         'approved', 'identity-late-lower-r1'),
        ('fixture', 'late-lower', 3, 'active',
         timestamp '2026-01-02 00:00:00 UTC', timestamp '2026-01-02 00:00:00 UTC',
         'approved', 'identity-late-lower-r3'),
        ('fixture', 'late-lower', 2, 'active',
         timestamp '2026-01-03 00:00:00 UTC', timestamp '2026-01-03 00:00:00 UTC',
         'approved', 'identity-late-lower-r2'),
        ('fixture', 'cancelled', 1, 'active',
         timestamp '2026-01-01 00:00:00 UTC', timestamp '2026-01-01 00:00:00 UTC',
         'approved', 'identity-cancelled-r1'),
        ('fixture', 'cancelled', 2, 'cancelled',
         timestamp '2026-01-02 00:00:00 UTC', timestamp '2026-01-02 00:00:00 UTC',
         'approved', 'identity-cancelled-r2')
),
revision_actual as (
    {{ record_high_knowledge_history(
        'revision_fixture',
        ['source_system_id', 'logical_id']
    ) }}
),
revision_expected (
    logical_id,
    source_revision,
    record_status,
    known_from_utc,
    known_to_utc
) as (
    values
        ('late-lower', 1, 'active', timestamp '2026-01-01 00:00:00 UTC',
         timestamp '2026-01-02 00:00:00 UTC'),
        ('late-lower', 3, 'active', timestamp '2026-01-02 00:00:00 UTC',
         cast(null as timestamp(6) with time zone)),
        ('cancelled', 1, 'active', timestamp '2026-01-01 00:00:00 UTC',
         timestamp '2026-01-02 00:00:00 UTC'),
        ('cancelled', 2, 'cancelled', timestamp '2026-01-02 00:00:00 UTC',
         cast(null as timestamp(6) with time zone))
),
revision_mismatches as (
    select 'missing_or_changed_revision_state' as failure
    from revision_expected as expected
    left join revision_actual as actual
      on expected.logical_id = actual.logical_id
     and expected.source_revision = actual.source_revision
    where actual.logical_id is null
       or actual.record_status is distinct from expected.record_status
       or actual.known_from_utc is distinct from expected.known_from_utc
       or actual.known_to_utc is distinct from expected.known_to_utc

    union all

    select 'unexpected_revision_state' as failure
    from revision_actual as actual
    left join revision_expected as expected
      on expected.logical_id = actual.logical_id
     and expected.source_revision = actual.source_revision
    where expected.logical_id is null
),

capacity_fixture (
    source_system_id,
    delivery_point_natural_id,
    interval_start_utc,
    source_revision,
    assessment_status,
    published_at_utc,
    approved_at_utc,
    approval_state,
    pipeline_identity_sha256
) as (
    values
        ('fixture', 'final-then-provisional', timestamp '2026-01-10 00:00:00 UTC',
         1, 'final', timestamp '2026-01-01 00:00:00 UTC',
         timestamp '2026-01-01 00:00:00 UTC', 'approved', 'cap-final-r1'),
        ('fixture', 'final-then-provisional', timestamp '2026-01-10 00:00:00 UTC',
         2, 'provisional', timestamp '2026-01-02 00:00:00 UTC',
         timestamp '2026-01-02 00:00:00 UTC', 'approved', 'cap-provisional-r2'),
        ('fixture', 'final-then-withdrawn', timestamp '2026-01-10 00:00:00 UTC',
         1, 'final', timestamp '2026-01-01 00:00:00 UTC',
         timestamp '2026-01-01 00:00:00 UTC', 'approved', 'cap-withdraw-r1'),
        ('fixture', 'final-then-withdrawn', timestamp '2026-01-10 00:00:00 UTC',
         2, 'withdrawn', timestamp '2026-01-02 00:00:00 UTC',
         timestamp '2026-01-02 00:00:00 UTC', 'approved', 'cap-withdraw-r2')
),
capacity_actual as (
    {{ capacity_assessment_knowledge_history('capacity_fixture') }}
),
capacity_expected (
    delivery_point_natural_id,
    source_revision,
    assessment_status,
    known_from_utc,
    known_to_utc
) as (
    values
        ('final-then-provisional', 1, 'final',
         timestamp '2026-01-01 00:00:00 UTC',
         cast(null as timestamp(6) with time zone)),
        ('final-then-withdrawn', 1, 'final',
         timestamp '2026-01-01 00:00:00 UTC', timestamp '2026-01-02 00:00:00 UTC'),
        ('final-then-withdrawn', 2, 'withdrawn',
         timestamp '2026-01-02 00:00:00 UTC',
         cast(null as timestamp(6) with time zone))
),
capacity_mismatches as (
    select 'missing_or_changed_capacity_state' as failure
    from capacity_expected as expected
    left join capacity_actual as actual
      on expected.delivery_point_natural_id = actual.delivery_point_natural_id
     and expected.source_revision = actual.source_revision
    where actual.delivery_point_natural_id is null
       or actual.assessment_status is distinct from expected.assessment_status
       or actual.known_from_utc is distinct from expected.known_from_utc
       or actual.known_to_utc is distinct from expected.known_to_utc

    union all

    select 'unexpected_capacity_state' as failure
    from capacity_actual as actual
    left join capacity_expected as expected
      on expected.delivery_point_natural_id = actual.delivery_point_natural_id
     and expected.source_revision = actual.source_revision
    where expected.delivery_point_natural_id is null
)

select * from revision_mismatches
union all
select * from capacity_mismatches
