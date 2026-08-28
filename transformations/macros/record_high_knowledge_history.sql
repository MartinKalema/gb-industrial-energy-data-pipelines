{% macro record_high_knowledge_history(relation, identity_fields, extra_eligibility='true') -%}
with eligible_revisions as (
    select
        source.*,
        {{ knowledge_eligible_at('published_at_utc', 'approved_at_utc') }}
            as knowledge_eligible_at_utc
    from {{ relation }} as source
    where approval_state = 'approved'
      and published_at_utc is not null
      and approved_at_utc is not null
      and ({{ extra_eligibility }})
),

one_revision_per_clock as (
    select *
    from (
        select
            eligible_revisions.*,
            row_number() over (
                partition by
                    {% for field in identity_fields %}
                    {{ field }},
                    {% endfor %}
                    knowledge_eligible_at_utc
                order by
                    source_revision desc,
                    pipeline_identity_sha256 desc
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
                {% for field in identity_fields %}
                {{ field }}{% if not loop.last %}, {% endif %}
                {% endfor %}
            order by knowledge_eligible_at_utc, source_revision
            rows between unbounded preceding and 1 preceding
        ) as prior_max_source_revision
    from one_revision_per_clock
),

authoritative_transitions as (
    select *
    from record_breakers
    where prior_max_source_revision is null
       or source_revision > prior_max_source_revision
)

select
    authoritative_transitions.*,
    knowledge_eligible_at_utc as known_from_utc,
    lead(knowledge_eligible_at_utc) over (
        partition by
            {% for field in identity_fields %}
            {{ field }}{% if not loop.last %}, {% endif %}
            {% endfor %}
        order by knowledge_eligible_at_utc, source_revision
    ) as known_to_utc
from authoritative_transitions
{%- endmacro %}
