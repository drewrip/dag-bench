{% macro calculate_age(start_date, end_date) %}
{{ dagbench_datediff('day', (start_date), (end_date)) }}
{% endmacro %}