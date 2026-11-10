{% macro safe_cast(column, to_type, default=none) %}
    COALESCE(
        {{ dagbench_try_cast((column), (to_type)) }},
        {% if default is not none %}{{ default }}{% else %}NULL{% endif %}
    )
{% endmacro %}
