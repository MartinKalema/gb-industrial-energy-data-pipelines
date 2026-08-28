with successful_coverage as (
    select *
    from {{ source('control', 'batch_run_coverage') }}
    where reconciliation_status in ('succeeded', 'succeeded_with_quarantine')
),

operating_dates as (
    select
        coverage.*,
        operating_date
    from successful_coverage as coverage
    cross join unnest(
        sequence(
            start_date_local_inclusive,
            end_date_local_inclusive,
            interval '1' day
        )
    ) as expanded_dates(operating_date)
),

date_boundaries as (
    select
        operating_dates.*,
        at_timezone(
            with_timezone(cast(operating_date as timestamp), operating_timezone),
            'UTC'
        ) as operating_date_start_utc,
        at_timezone(
            with_timezone(
                cast(date_add('day', 1, operating_date) as timestamp),
                operating_timezone
            ),
            'UTC'
        ) as next_operating_date_start_utc
    from operating_dates
),

coverage_intervals as (
    select
        date_boundaries.*,
        period_number,
        cast(
            date_add(
                'minute',
                period_number * 30,
                operating_date_start_utc
            )
            as timestamp(6) with time zone
        ) as interval_start_utc,
        cast(
            date_add(
                'minute',
                (period_number + 1) * 30,
                operating_date_start_utc
            )
            as timestamp(6) with time zone
        ) as interval_end_utc
    from date_boundaries
    cross join unnest(
        sequence(
            cast(0 as bigint),
            cast(
                date_diff(
                    'minute',
                    operating_date_start_utc,
                    next_operating_date_start_utc
                ) / 30 - 1
                as bigint
            )
        )
    ) as expanded_intervals(period_number)
),

assigned_intervals as (
    select
        assignment.source_system_id as delivery_point_source_system_id,
        assignment.delivery_point_assignment_id,
        assignment.delivery_point_natural_id,
        assignment.delivery_point_name,
        assignment.site_natural_id,
        assignment.customer_natural_id,
        assignment.service_type,
        assignment.source_revision as delivery_point_assignment_source_revision,
        assignment.revision_type as delivery_point_assignment_revision_type,
        assignment.published_at_utc as delivery_point_assignment_published_at_utc,
        assignment.approved_at_utc as delivery_point_assignment_approved_at_utc,
        assignment.pipeline_payload_sha256
            as delivery_point_assignment_pipeline_payload_sha256,
        coverage.pipeline_run_id as coverage_pipeline_run_id,
        coverage.operating_date as reporting_date,
        coverage.operating_timezone,
        coverage.interval_start_utc,
        coverage.interval_end_utc,
        coverage.period_number + 1 as local_period_number,
        coverage.coverage_published_at_utc,
        coverage.raw_manifest_uri,
        coverage.raw_manifest_sha256,
        coverage.reconciliation_artifact_uri,
        coverage.reconciliation_artifact_sha256
    from coverage_intervals as coverage
    inner join {{ ref('int_current_delivery_point_assignments') }} as assignment
      on assignment.effective_from_utc <= coverage.interval_start_utc
     and assignment.assignment_status = 'active'
     and (
            assignment.effective_to_utc is null
            or coverage.interval_end_utc <= assignment.effective_to_utc
         )
),

deduplicated as (
    select
        delivery_point_source_system_id,
        delivery_point_assignment_id,
        delivery_point_natural_id,
        delivery_point_name,
        site_natural_id,
        customer_natural_id,
        service_type,
        delivery_point_assignment_source_revision,
        delivery_point_assignment_revision_type,
        delivery_point_assignment_published_at_utc,
        delivery_point_assignment_approved_at_utc,
        delivery_point_assignment_pipeline_payload_sha256,
        reporting_date,
        operating_timezone,
        interval_start_utc,
        interval_end_utc,
        local_period_number,
        count(distinct coverage_pipeline_run_id) as coverage_run_count,
        array_join(
            array_sort(array_distinct(array_agg(coverage_pipeline_run_id))),
            ','
        ) as coverage_pipeline_run_ids,
        min(coverage_published_at_utc) as first_coverage_published_at_utc,
        max(coverage_published_at_utc) as latest_coverage_published_at_utc,
        array_join(
            array_sort(array_distinct(array_agg(raw_manifest_uri))),
            ','
        ) as coverage_raw_manifest_uris,
        array_join(
            array_sort(array_distinct(array_agg(raw_manifest_sha256))),
            ','
        ) as coverage_raw_manifest_sha256s,
        array_join(
            array_sort(array_distinct(array_agg(reconciliation_artifact_uri))),
            ','
        ) as coverage_reconciliation_artifact_uris,
        array_join(
            array_sort(array_distinct(array_agg(reconciliation_artifact_sha256))),
            ','
        ) as coverage_reconciliation_artifact_sha256s
    from assigned_intervals
    group by
        delivery_point_source_system_id,
        delivery_point_assignment_id,
        delivery_point_natural_id,
        delivery_point_name,
        site_natural_id,
        customer_natural_id,
        service_type,
        delivery_point_assignment_source_revision,
        delivery_point_assignment_revision_type,
        delivery_point_assignment_published_at_utc,
        delivery_point_assignment_approved_at_utc,
        delivery_point_assignment_pipeline_payload_sha256,
        reporting_date,
        operating_timezone,
        interval_start_utc,
        interval_end_utc,
        local_period_number
)

select
    {{ sha256_key([
        'delivery_point_natural_id',
        'to_iso8601(interval_start_utc)'
    ]) }} as delivery_interval_key,
    *
from deduplicated
