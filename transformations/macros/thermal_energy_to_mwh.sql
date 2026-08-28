{% macro thermal_energy_to_mwh(value_expression, unit_expression) -%}
    cast(
        case {{ unit_expression }}
            when 'MWh_th' then {{ value_expression }}
            when 'kWh_th' then {{ value_expression }} / decimal '1000.000000'
            when 'GJ_th' then {{ value_expression }} / decimal '3.600000'
        end
        as decimal(20, 6)
    )
{%- endmacro %}
