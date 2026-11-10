{% macro standardize_date(column, formats=['%Y-%m-%d', '%m/%d/%Y', '%d-%b-%Y']) %}
    COALESCE(
        {{ dagbench_try_cast((column), "DATE") }},
        {% for fmt in formats %}
        {{ dagbench_strptime((column), "'" ~ (fmt) ~ "'", try=True) }}{% if not loop.last %},{% endif %}
        {% endfor %}
    )
{% endmacro %}
