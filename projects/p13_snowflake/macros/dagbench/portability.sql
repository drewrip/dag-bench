{#
    dag-bench: DuckDB-only constructs in upstream's models, rewritten into these
    calls by utils/retail_vendor.py. On DuckDB each renders upstream's SQL (or,
    for hash and "now", a value both engines compute identically).
#}

{# upstream: date_diff('<part>', a, b) / datediff(...) #}
{% macro dagbench_datediff(datepart, first_date, second_date) -%}
    {%- if target.type == 'duckdb' -%}
        date_diff('{{ datepart }}', {{ first_date }}, {{ second_date }})
    {%- else -%}
        {{ dbt.datediff(first_date, second_date, datepart) }}
    {%- endif -%}
{%- endmacro %}

{# upstream: TRY_CAST(expr AS type): NULL when expr doesn't parse as type.
   Postgres (16+) checks with pg_input_is_valid. Its date/time parser accepts
   more than DuckDB's, and upstream's data has the difference on purpose
   ('07/23/2020', '20231117', epoch strings: NULL in DuckDB), so dates and
   timestamps must also have DuckDB's shape: Y-M-D (or Y/M/D), then for a
   timestamp an optional time and zone; a date ignores what follows. #}
{% macro dagbench_try_cast(expr, type) -%}
    {%- set t = type | lower | trim -%}
    {#- DuckDB's bare DECIMAL is DECIMAL(18,3); Postgres' is unconstrained. -#}
    {%- set type = 'decimal(18,3)' if t == 'decimal' else type -%}
    {%- set ymd = "[0-9]{4}[-/][0-9]{1,2}[-/][0-9]{1,2}" -%}
    {%- if target.type == 'duckdb' -%}
        TRY_CAST({{ expr }} AS {{ type }})
    {%- elif t == 'timestamp' -%}
        case when cast({{ expr }} as text) ~ '^{{ ymd }}([ T][0-9]{1,2}:[0-9]{1,2}(:[0-9]{1,2}([.][0-9]+)?)?)?(Z| ?[+-][0-9]{2}(:?[0-9]{2})?| UTC)?$'
              and pg_input_is_valid(cast({{ expr }} as text), 'timestamp')
             then cast({{ expr }} as timestamp) end
    {%- elif t == 'date' -%}
        case when pg_input_is_valid(substring(cast({{ expr }} as text) from '^ *({{ ymd }})'), 'date')
             then cast(substring(cast({{ expr }} as text) from '^ *({{ ymd }})') as date) end
    {%- else -%}
        case when pg_input_is_valid(cast({{ expr }} as text), '{{ type }}') then cast({{ expr }} as {{ type }}) end
    {%- endif -%}
{%- endmacro %}

{# upstream: strptime / TRY_STRPTIME(expr, '<strftime format>'): a timestamp.
   Postgres: to_timestamp with the format translated; the TRY_ form returns NULL
   unless expr has the format's shape. Only the specifiers upstream uses. #}
{% macro dagbench_strptime(expr, fmt, try=false) -%}
    {%- if target.type == 'duckdb' -%}
        {{ 'TRY_STRPTIME' if try else 'strptime' }}({{ expr }}, {{ fmt }})
    {%- else -%}
        {%- set f = fmt.strip() -%}
        {%- if not (f.startswith("'") and f.endswith("'")) -%}
            {{ exceptions.raise_compiler_error("dagbench_strptime: format must be a literal, got " ~ fmt) }}
        {%- endif -%}
        {%- set f = f[1:-1] -%}
        {%- set pg = f.replace('%Y', 'YYYY').replace('%m', 'MM').replace('%d', 'DD').replace('%H', 'HH24').replace('%M', 'MI').replace('%S', 'SS').replace('%b', 'Mon') -%}
        {%- set ts = "cast(to_timestamp(" ~ expr ~ ", '" ~ pg ~ "') as timestamp)" -%}
        {%- if try -%}
            {%- set n = '[0-9]{1,2}' -%}
            {%- set re = f.replace('.', '[.]').replace('%Y', '[0-9]{4}').replace('%b', '[A-Za-z]{3}')
                          .replace('%m', n).replace('%d', n).replace('%H', n).replace('%M', n).replace('%S', n) -%}
            case when {{ expr }} ~ '^{{ re }}$' then {{ ts }} end
        {%- else -%}
            {{ ts }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}

{# upstream: regexp_matches(s, pattern), a boolean partial match in DuckDB
   (Postgres' regexp_matches returns the matches). #}
{% macro dagbench_regexp_matches(s, pattern) -%}
    {%- if target.type == 'duckdb' -%}
        regexp_matches({{ s }}, {{ pattern }})
    {%- else -%}
        ({{ s }} ~ {{ pattern }})
    {%- endif -%}
{%- endmacro %}

{# upstream: epoch_ms(ms), a UTC timestamp. #}
{% macro dagbench_epoch_ms(ms) -%}
    {%- if target.type == 'duckdb' -%}
        epoch_ms({{ ms }})
    {%- else -%}
        (to_timestamp(({{ ms }}) / 1000.0) at time zone 'UTC')
    {%- endif -%}
{%- endmacro %}

{# upstream: hash(expr), DuckDB's 64-bit hash, used to fake columns from ids.
   Replaced by the first 60 bits of md5 (non-negative bigint), the same on both. #}
{% macro dagbench_hash(expr) -%}
    {%- if target.type == 'duckdb' -%}
        cast(('0x' || substr(md5(cast({{ expr }} as varchar)), 1, 15)) as bigint)
    {%- else -%}
        cast(cast(('x' || substr(md5(cast({{ expr }} as text)), 1, 15)) as bit(60)) as bigint)
    {%- endif -%}
{%- endmacro %}

{# upstream: current_date / current_timestamp, pinned to the data's as-of date
   so results don't depend on the day the DAG runs. #}
{% macro dagbench_current_date() -%}
    cast('{{ var("dagbench_as_of_date") }}' as date)
{%- endmacro %}

{% macro dagbench_current_timestamp() -%}
    cast('{{ var("dagbench_as_of_date") }}' as timestamp)
{%- endmacro %}

{# upstream: round(<double>, n). Postgres has round(numeric, int) only. #}
{% macro dagbench_round_double(expr, digits) -%}
    {%- if target.type == 'duckdb' -%}
        round({{ expr }}, {{ digits }})
    {%- else -%}
        cast(round(cast({{ expr }} as numeric), {{ digits }}) as double precision)
    {%- endif -%}
{%- endmacro %}

{# upstream: DuckDB date part functions. dayofweek: 0 = Sunday on both. #}
{% macro dagbench_dayofweek(d) -%}
    {%- if target.type == 'duckdb' -%} dayofweek({{ d }}) {%- else -%} extract(dow from {{ d }}) {%- endif -%}
{%- endmacro %}

{% macro dagbench_dayname(d) -%}
    {%- if target.type == 'duckdb' -%} dayname({{ d }}) {%- else -%} to_char({{ d }}, 'FMDay') {%- endif -%}
{%- endmacro %}

{% macro dagbench_monthname(d) -%}
    {%- if target.type == 'duckdb' -%} monthname({{ d }}) {%- else -%} to_char({{ d }}, 'FMMonth') {%- endif -%}
{%- endmacro %}

{% macro dagbench_last_day(d) -%}
    {%- if target.type == 'duckdb' -%}
        last_day({{ d }})
    {%- else -%}
        cast(date_trunc('month', {{ d }}) + interval '1 month' - interval '1 day' as date)
    {%- endif -%}
{%- endmacro %}

{# upstream: RANDOM(). A uniform [0, 1) value seeded by `seed`, the same on
   every run and engine. #}
{% macro dagbench_random(seed) -%}
    (cast({{ dagbench_hash(seed) }} % 1000000 as double precision) / 1000000.0)
{%- endmacro %}
