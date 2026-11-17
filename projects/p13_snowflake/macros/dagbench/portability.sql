{#
    dag-bench: DuckDB-only constructs in upstream's models, rewritten into these
    calls by utils/retail_vendor.py. On DuckDB each renders upstream's SQL (or,
    for hash and "now", a value both engines compute identically). The `else`
    branches are Postgres; Snowflake has its own branch where
    the Postgres SQL doesn't fit.
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
    {%- elif target.type == 'snowflake' -%}
        {#- Snowflake's TRY_CAST takes strings only, and its parsers accept shapes
            DuckDB rejects ('20231117', '07/23/2020', '1.5e3' ...): same shape
            guards as Postgres. Patterns hold no backslashes (they would be
            string escapes there); regexp_like matches the whole string. -#}
        {%- set s = "cast(" ~ expr ~ " as varchar)" -%}
        {%- if t == 'timestamp' -%}
            case when regexp_like({{ s }}, '{{ ymd }}([ T][0-9]{1,2}:[0-9]{1,2}(:[0-9]{1,2}([.][0-9]+)?)?)?(Z| ?[+-][0-9]{2}(:?[0-9]{2})?| UTC)?')
                 then try_cast(replace({{ s }}, '/', '-') as timestamp) end
        {%- elif t == 'date' -%}
            try_cast(replace(regexp_substr({{ s }}, '^ *({{ ymd }})', 1, 1, 'e', 1), '/', '-') as date)
        {%- elif t in ('boolean', 'bool') -%}
            {#- DuckDB: t/true/1/y/yes and f/false/0/n/no, any case, untrimmed
                (Snowflake also takes on/off). -#}
            case when lower({{ s }}) in ('t', 'true', '1', 'y', 'yes') then true
                 when lower({{ s }}) in ('f', 'false', '0', 'n', 'no') then false end
        {%- elif t in ('varchar', 'text', 'string') -%}
            {{ s }}
        {%- elif t.split('(')[0] in ('integer', 'int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'double', 'double precision', 'float', 'real') -%}
            {#- DuckDB trims, takes exponents and rounds to the target scale;
                floats also take inf / nan. -#}
            case when regexp_like({{ s }}, ' *[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)? *'{% if t in ('double', 'double precision', 'float', 'real') %}, 'i') or regexp_like({{ s }}, ' *[+-]?(inf|infinity|nan) *', 'i'{% endif %})
                 then try_cast(trim({{ s }}) as {{ type }}) end
        {%- else -%}
            try_cast({{ s }} as {{ type }})
        {%- endif -%}
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
    {%- elif target.type == 'snowflake' -%}
        {%- set f = fmt.strip()[1:-1] -%}
        {%- set sf = f.replace('%Y', 'YYYY').replace('%m', 'MM').replace('%d', 'DD').replace('%H', 'HH24').replace('%M', 'MI').replace('%S', 'SS').replace('%b', 'MON') -%}
        {%- if try -%}
            {%- set n = '[0-9]{1,2}' -%}
            {%- set re = f.replace('.', '[.]').replace('%Y', '[0-9]{4}').replace('%b', '[A-Za-z]{3}')
                          .replace('%m', n).replace('%d', n).replace('%H', n).replace('%M', n).replace('%S', n) -%}
            case when regexp_like({{ expr }}, '{{ re }}') then try_to_timestamp_ntz({{ expr }}, '{{ sf }}') end
        {%- else -%}
            to_timestamp_ntz({{ expr }}, '{{ sf }}')
        {%- endif -%}
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
    {%- elif target.type == 'snowflake' -%}
        {#- Partial match; backslashes in a Snowflake string literal are escapes. -#}
        (regexp_instr({{ s }}, {{ pattern | replace('\\', '\\\\') }}) > 0)
    {%- else -%}
        ({{ s }} ~ {{ pattern }})
    {%- endif -%}
{%- endmacro %}

{# upstream: epoch_ms(ms), a UTC timestamp. #}
{% macro dagbench_epoch_ms(ms) -%}
    {%- if target.type == 'duckdb' -%}
        epoch_ms({{ ms }})
    {%- elif target.type == 'snowflake' -%}
        to_timestamp_ntz({{ ms }}, 3)
    {%- else -%}
        (to_timestamp(({{ ms }}) / 1000.0) at time zone 'UTC')
    {%- endif -%}
{%- endmacro %}

{# upstream: hash(expr), DuckDB's 64-bit hash, used to fake columns from ids.
   Replaced by the first 60 bits of md5 (non-negative bigint), the same on both. #}
{% macro dagbench_hash(expr) -%}
    {%- if target.type == 'duckdb' -%}
        cast(('0x' || substr(md5(cast({{ expr }} as varchar)), 1, 15)) as bigint)
    {%- elif target.type == 'snowflake' -%}
        to_number(substr(md5(cast({{ expr }} as varchar)), 1, 15), 'XXXXXXXXXXXXXXX')
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
    {%- elif target.type == 'snowflake' -%}
        {#- Postgres' cast to numeric would be NUMBER(38,0) here. -#}
        round({{ expr }}, {{ digits }})
    {%- else -%}
        cast(round(cast({{ expr }} as numeric), {{ digits }}) as double precision)
    {%- endif -%}
{%- endmacro %}

{# upstream: DuckDB date part functions. dayofweek: 0 = Sunday on both. #}
{% macro dagbench_dayofweek(d) -%}
    {%- if target.type == 'duckdb' -%} dayofweek({{ d }})
    {%- elif target.type == 'snowflake' -%} (dayofweekiso({{ d }}) % 7) {#- independent of WEEK_START -#}
    {%- else -%} extract(dow from {{ d }}) {%- endif -%}
{%- endmacro %}

{% macro dagbench_dayname(d) -%}
    {%- if target.type == 'duckdb' -%} dayname({{ d }})
    {%- elif target.type == 'snowflake' -%} decode(dayofweekiso({{ d }}), 1, 'Monday', 2, 'Tuesday', 3, 'Wednesday', 4, 'Thursday', 5, 'Friday', 6, 'Saturday', 7, 'Sunday')
    {%- else -%} to_char({{ d }}, 'FMDay') {%- endif -%}
{%- endmacro %}

{% macro dagbench_monthname(d) -%}
    {%- if target.type == 'duckdb' -%} monthname({{ d }})
    {%- elif target.type == 'snowflake' -%} decode(month({{ d }}), 1, 'January', 2, 'February', 3, 'March', 4, 'April', 5, 'May', 6, 'June', 7, 'July', 8, 'August', 9, 'September', 10, 'October', 11, 'November', 12, 'December')
    {%- else -%} to_char({{ d }}, 'FMMonth') {%- endif -%}
{%- endmacro %}

{% macro dagbench_last_day(d) -%}
    {%- if target.type == 'duckdb' -%}
        last_day({{ d }})
    {%- elif target.type == 'snowflake' -%}
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

{# Snowflake --------------------------------------------------------------- #}

{# upstream: INTERVAL (n) DAY, rewritten by utils/retail_vendor.py to
   (n) * INTERVAL '1 day'. Snowflake can't scale an interval; every use adds
   days to a DATE, which Snowflake does with date + n. #}
{% macro dagbench_interval_n(n, unit) -%}
    {%- if target.type == 'snowflake' -%}
        {%- if unit != 'day' -%}
            {{ exceptions.raise_compiler_error("dagbench_interval_n: only day on snowflake, got " ~ unit) }}
        {%- endif -%}
        ({{ n }})
    {%- else -%}
        ({{ n }}) * INTERVAL '1 {{ unit }}'
    {%- endif -%}
{%- endmacro %}

{# upstream: FROM generate_series(CAST('<start>' AS TIMESTAMP), <now>, INTERVAL '1 day') AS spine(d),
   a day spine up to dagbench_as_of_date. #}
{% macro dagbench_day_spine_from(start) -%}
    {%- if target.type == 'snowflake' -%}
        {%- set d0 = modules.datetime.date.fromisoformat(start) -%}
        {%- set d1 = modules.datetime.date.fromisoformat(var("dagbench_as_of_date")) -%}
        FROM (select dateadd(day, row_number() over (order by seq4()) - 1, CAST('{{ start }}' AS TIMESTAMP)) as d
              from table(generator(rowcount => {{ (d1 - d0).days + 1 }}))) AS spine
    {%- else -%}
        FROM generate_series(CAST('{{ start }}' AS TIMESTAMP), CAST({{ dagbench_current_date() }} AS TIMESTAMP), INTERVAL '1 day') AS spine(d)
    {%- endif -%}
{%- endmacro %}

{# upstream: date_part('year', age(a, b)), whole years from b to a. #}
{% macro dagbench_age_years(a, b) -%}
    {%- if target.type == 'snowflake' -%}
        (datediff(year, {{ b }}, {{ a }}) - case when dateadd(year, datediff(year, {{ b }}, {{ a }}), {{ b }}) > {{ a }} then 1 else 0 end)
    {%- else -%}
        date_part('year', age({{ a }}, {{ b }}))
    {%- endif -%}
{%- endmacro %}
