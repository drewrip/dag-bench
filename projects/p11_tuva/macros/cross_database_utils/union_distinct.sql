{#
    Emits the adapter-specific spelling for a duplicate-eliminating set union.

    BigQuery requires the DISTINCT keyword, while Fabric's T-SQL grammar
    rejects it because bare UNION is already duplicate-eliminating.
#}
{% macro union_distinct() -%}
    {{ return(adapter.dispatch('union_distinct', 'p11_tuva')()) }}
{%- endmacro %}

{% macro default__union_distinct() -%}
    union distinct
{%- endmacro %}

{% macro fabric__union_distinct() -%}
    union
{%- endmacro %}

{# SQL Server is T-SQL. dbt-sqlserver registers no adapter-type parent,
   so fabric__ macros are not reached by dispatch and must be aliased. #}
{% macro sqlserver__union_distinct() -%}
    {{ p11_tuva.fabric__union_distinct() }}
{%- endmacro %}
