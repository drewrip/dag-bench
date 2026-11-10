{% macro get_business_days(start_date, end_date) %}
{{ dagbench_datediff('day', (start_date), (end_date)) }} * 5/7
{% endmacro %}