{% macro knowledge_eligible_at(published_at, approved_at) -%}
    greatest({{ published_at }}, {{ approved_at }})
{%- endmacro %}
