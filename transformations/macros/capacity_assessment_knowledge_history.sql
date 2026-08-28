{% macro capacity_assessment_knowledge_history(input_relation) -%}
with eligible_revisions as (
    select
        source.*,
        {{ knowledge_eligible_at('published_at_utc', 'approved_at_utc') }}
            as knowledge_eligible_at_utc
    from {{ input_relation }} as source
    where approval_state = 'approved'
      and published_at_utc is not null
      and approved_at_utc is not null
      and assessment_status in ('provisional', 'final', 'withdrawn')
),

one_revision_per_clock as (
    select *
    from (
        select
            eligible_revisions.*,
            row_number() over (
                partition by
                    source_system_id,
                    delivery_point_natural_id,
                    interval_start_utc,
                    knowledge_eligible_at_utc
                order by source_revision desc, pipeline_identity_sha256 desc
            ) as knowledge_clock_rank
        from eligible_revisions
    )
    where knowledge_clock_rank = 1
),

record_breakers as (
    select
        one_revision_per_clock.*,
        max(source_revision) over (
            partition by
                source_system_id,
                delivery_point_natural_id,
                interval_start_utc
            order by knowledge_eligible_at_utc, source_revision
            rows between unbounded preceding and 1 preceding
        ) as prior_max_source_revision
    from one_revision_per_clock
),

authoritative_record_breakers as (
    select *
    from record_breakers
    where prior_max_source_revision is null
       or source_revision > prior_max_source_revision
),

with_official_state as (
    select
        authoritative_record_breakers.*,
        count_if(assessment_status in ('final', 'withdrawn')) over (
            partition by
                source_system_id,
                delivery_point_natural_id,
                interval_start_utc
            order by knowledge_eligible_at_utc, source_revision
            rows between unbounded preceding and current row
        ) as official_state_count
    from authoritative_record_breakers
),

state_transitions as (
    select *
    from with_official_state
    where assessment_status in ('final', 'withdrawn')
       or (assessment_status = 'provisional' and official_state_count = 0)
)

select
    state_transitions.*,
    knowledge_eligible_at_utc as known_from_utc,
    lead(knowledge_eligible_at_utc) over (
        partition by
            source_system_id,
            delivery_point_natural_id,
            interval_start_utc
        order by knowledge_eligible_at_utc, source_revision
    ) as known_to_utc
from state_transitions
{%- endmacro %}
