-- based off of  https://github.com/calogica/dbt-date/blob/0.7.1/macros/calendar_date/date_part.sql

{% macro date_part(datepart, date) -%}
    {{ adapter.dispatch('date_part', 'p11_tuva') (datepart, date) }}
{%- endmacro %}

{% macro default__date_part(datepart, date) -%}
    date_part('{{ datepart }}', {{  date }})
{%- endmacro %}

{% macro bigquery__date_part(datepart, date) -%}
    extract({{ datepart }} from {{ date }})
{%- endmacro %}

{% macro athena__date_part(datepart, date) -%}
    extract({{ datepart }} from {{ date }})
{%- endmacro %}

{% macro fabric__date_part(datepart, date) %}
    {% if datepart == 'year' %}
        YEAR({{ date }})
    {% elif datepart == 'month' %}
        MONTH({{ date }})
    {% else %}
        NULL
    {% endif %}
{% endmacro %}

{# SQL Server is T-SQL. dbt-sqlserver registers no adapter-type parent,
   so fabric__ macros are not reached by dispatch and must be aliased. #}
{% macro sqlserver__date_part(datepart, date) -%}
    {{ p11_tuva.fabric__date_part(datepart, date) }}
{%- endmacro %}
