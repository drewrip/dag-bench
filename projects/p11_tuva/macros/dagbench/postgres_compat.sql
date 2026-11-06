{#
    dag-bench: Postgres fixes for tuva-core. utils/tuva_vendor.py deletes the
    same-named upstream macros, so these replace them.

    Upstream's postgres__try_to_cast_* use dbt.safe_cast, which on Postgres is a
    plain cast and raises on bad input (e.g. HCPCS '1036F' -> integer). These
    return null instead, accepting the input forms DuckDB's try_cast accepts for
    Tuva's data: optionally signed digits for integers, ISO dates/timestamps.
    (DuckDB also rounds decimal strings like '12.5' to integers; codes never
    contain those, and they are null here.)
#}

{%- macro postgres__try_to_cast_int(column_name) -%}
    case
        when cast({{ column_name }} as text) ~ '^\s*[+-]?[0-9]+\s*$'
         and pg_input_is_valid(cast({{ column_name }} as text), 'integer')
        then cast(cast({{ column_name }} as text) as integer)
    end
{%- endmacro -%}

{%- macro postgres__try_to_cast_date(column_name, date_format) -%}
    case
        when cast({{ column_name }} as text) ~ '^\s*[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}([ T].*)?$'
         and pg_input_is_valid(cast({{ column_name }} as text), 'date')
        then cast(cast({{ column_name }} as text) as date)
    end
{%- endmacro -%}

{%- macro postgres__try_to_cast_datetime(column_name) -%}
    case
        when cast({{ column_name }} as text) ~ '^\s*[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}([ T].*)?$'
         and pg_input_is_valid(cast({{ column_name }} as text), 'timestamp')
        then cast(cast({{ column_name }} as text) as timestamp)
    end
{%- endmacro -%}
