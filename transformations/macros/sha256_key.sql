{% macro sha256_key(components) -%}
    {#
      Length-prefix every component before hashing so values such as
      ('ab', 'c') and ('a', 'bc') can never serialize to the same input.
      concat() propagates null in Trino, deliberately leaving the key null
      when a required business-key component is absent.
    #}
    case
        when
            {%- for component in components %}
            ({{ component }}) is null{% if not loop.last %} or {% endif %}
            {%- endfor %}
            then cast(null as varchar)
        else lower(
            to_hex(
                sha256(
                    to_utf8(
                        array_join(
                            array[
                                {%- for component in components %}
                                concat(
                                    cast(
                                        length(cast({{ component }} as varchar))
                                        as varchar
                                    ),
                                    ':',
                                    cast({{ component }} as varchar)
                                ){% if not loop.last %}, {% endif %}
                                {%- endfor %}
                            ],
                            '|'
                        )
                    )
                )
            )
        )
    end
{%- endmacro %}
