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
            date_add('minute', period_number * 30, operating_date_start_utc)
            as timestamp(6) with time zone
        ) as interval_start_utc,
        cast(
            date_add('minute', (period_number + 1) * 30, operating_date_start_utc)
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

ever_active_delivery_points as (
    select distinct
        source.delivery_point_natural_id,
        source.effective_from_utc,
        source.effective_to_utc
    from {{ ref('int_delivery_point_assignments_knowledge_history') }} as source
    where source.assignment_status = 'active'
),

covered_delivery_points as (
    select distinct
        delivery_point.delivery_point_natural_id,
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
    inner join ever_active_delivery_points as delivery_point
      on delivery_point.effective_from_utc <= coverage.interval_start_utc
     and (
            delivery_point.effective_to_utc is null
            or coverage.interval_end_utc <= delivery_point.effective_to_utc
         )
),

deduplicated as (
    select
        delivery_point_natural_id,
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
    from covered_delivery_points
    group by
        delivery_point_natural_id,
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
