{#

    Left-pads a fixed-width numeric code with zeros to the width Tuva
    terminology uses, for example '1' to '01' for discharge disposition or
    '65' to '065' for MS-DRG. Surrounding whitespace is removed first. A value
    already at or beyond the width is returned trimmed and never truncated,
    and a blank value is never padded into an all-zero code such as '00'.

#}

{% macro zero_pad_code(expression, width) %}
  {{ return(adapter.dispatch('zero_pad_code', 'p11_tuva')(expression, width)) }}
{% endmacro %}

{% macro default__zero_pad_code(expression, width) %}
  {%- set trimmed = p11_tuva.trim(expression) -%}
  case
    when {{ p11_tuva.length(trimmed) }} between 1 and {{ width - 1 }}
      then lpad({{ trimmed }}, {{ width }}, '0')
    else {{ trimmed }}
  end
{% endmacro %}

{% macro fabric__zero_pad_code(expression, width) %}
  {%- set trimmed = p11_tuva.trim(expression) -%}
  case
    when {{ p11_tuva.length(trimmed) }} between 1 and {{ width - 1 }}
      then right(replicate('0', {{ width }}) + {{ trimmed }}, {{ width }})
    else {{ trimmed }}
  end
{% endmacro %}

{# SQL Server is T-SQL. dbt-sqlserver registers no adapter-type parent,
   so fabric__ macros are not reached by dispatch and must be aliased. #}
{% macro sqlserver__zero_pad_code(expression, width) -%}
    {{ p11_tuva.fabric__zero_pad_code(expression, width) }}
{%- endmacro %}
