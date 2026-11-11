{#
    dag-bench: DuckDB-only constructs in upstream's models, rewritten into these
    calls by utils/astronomer_vendor.py. On DuckDB each renders upstream's SQL.
#}

{# upstream: date_diff('<part>', a, b), the number of part boundaries crossed,
   a bigint. dbt.datediff counts the same way; on Postgres it is a double
   precision for sub-day parts, so cast to DuckDB's type. #}
{% macro dagbench_datediff(datepart, first_date, second_date) -%}
    {%- if target.type == 'duckdb' -%}
        date_diff('{{ datepart }}', {{ first_date }}, {{ second_date }})
    {%- else -%}
        cast({{ dbt.datediff(first_date, second_date, datepart) }} as bigint)
    {%- endif -%}
{%- endmacro %}

{# upstream: strftime(date, '<format>'). Only the specifiers upstream uses. #}
{% macro dagbench_strftime(expr, fmt) -%}
    {%- if target.type == 'duckdb' -%}
        strftime({{ expr }}, {{ fmt }})
    {%- else -%}
        {%- set f = fmt.strip() -%}
        {%- if not (f.startswith("'") and f.endswith("'")) -%}
            {{ exceptions.raise_compiler_error("dagbench_strftime: format must be a literal, got " ~ fmt) }}
        {%- endif -%}
        to_char({{ expr }}, '{{ f[1:-1].replace('%Y', 'YYYY').replace('%m', 'MM').replace('%d', 'DD') }}')
    {%- endif -%}
{%- endmacro %}

{# upstream: json_extract_string(json_text, '$.key'): the key's value as text. #}
{% macro dagbench_json_extract_string(expr, path) -%}
    {%- if target.type == 'duckdb' -%}
        json_extract_string({{ expr }}, {{ path }})
    {%- else -%}
        {%- set p = path.strip() -%}
        {%- if not (p.startswith("'$.") and p.endswith("'")) -%}
            {{ exceptions.raise_compiler_error("dagbench_json_extract_string: path must be a '$.key' literal, got " ~ path) }}
        {%- endif -%}
        (cast({{ expr }} as json) ->> '{{ p[3:-1] }}')
    {%- endif -%}
{%- endmacro %}

{# upstream: string_split(s, sep), a list of strings. #}
{% macro dagbench_string_split(expr, sep) -%}
    {%- if target.type == 'duckdb' -%}
        string_split({{ expr }}, {{ sep }})
    {%- else -%}
        string_to_array({{ expr }}, {{ sep }})
    {%- endif -%}
{%- endmacro %}

{# upstream: list_contains(list, x) #}
{% macro dagbench_list_contains(list, x) -%}
    {%- if target.type == 'duckdb' -%}
        list_contains({{ list }}, {{ x }})
    {%- else -%}
        ({{ x }} = any({{ list }}))
    {%- endif -%}
{%- endmacro %}

{# upstream: isodow(date), 1 = Monday .. 7 = Sunday. #}
{% macro dagbench_isodow(expr) -%}
    {%- if target.type == 'duckdb' -%}
        isodow({{ expr }})
    {%- else -%}
        cast(extract(isodow from {{ expr }}) as integer)
    {%- endif -%}
{%- endmacro %}

{# upstream: dayname(date), the English day name ('Monday'). #}
{% macro dagbench_dayname(expr) -%}
    {%- if target.type == 'duckdb' -%}
        dayname({{ expr }})
    {%- else -%}
        to_char({{ expr }}, 'FMDay')
    {%- endif -%}
{%- endmacro %}
