{% set histories = [
    ('int_customer_versions_knowledge_history', ['source_system_id', 'customer_version_id']),
    ('int_site_versions_knowledge_history', ['source_system_id', 'site_version_id']),
    ('int_delivery_point_assignments_knowledge_history', ['source_system_id', 'delivery_point_assignment_id']),
    ('int_meter_assignments_knowledge_history', ['source_system_id', 'meter_assignment_id']),
    ('int_contract_terms_knowledge_history', ['source_system_id', 'contract_terms_version_id']),
    ('int_commitments_knowledge_history', ['source_system_id', 'delivery_point_natural_id', 'interval_start_utc']),
    ('int_eligible_excess_orders_knowledge_history', ['source_system_id', 'order_interval_line_id']),
    ('int_meter_readings_knowledge_history', ['source_system_id', 'meter_natural_id', 'register_natural_id', 'reading_at_utc']),
    ('int_capacity_assessments_knowledge_history', ['source_system_id', 'delivery_point_natural_id', 'interval_start_utc'])
] %}

with violations as (
    {% for model_name, identity_fields in histories %}
    select '{{ model_name }}' as model_name
    from (
        select
            source_revision,
            known_from_utc,
            known_to_utc,
            published_at_utc,
            approved_at_utc,
            lag(source_revision) over (
                partition by {{ identity_fields | join(', ') }}
                order by known_from_utc
            ) as prior_source_revision,
            lag(known_to_utc) over (
                partition by {{ identity_fields | join(', ') }}
                order by known_from_utc
            ) as prior_known_to_utc
        from {{ ref(model_name) }}
    ) as history
    where known_from_utc <> greatest(published_at_utc, approved_at_utc)
       or source_revision <= prior_source_revision
       or prior_known_to_utc > known_from_utc
       or (prior_known_to_utc is null and prior_source_revision is not null)
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select *
from violations
