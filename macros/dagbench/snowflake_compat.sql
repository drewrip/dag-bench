{#
  Snowflake portability for the synthetic projects (p01-p10, m01-m06), which
  pick these up through `macro-paths: ["macros", "../../macros"]`.

  Contract: on every target except snowflake each macro renders the exact SQL
  text the models had before, so compiled SQL for `dev` (DuckDB) and `postgres`
  stays byte-identical. utils/check_compiled_unchanged.sh enforces this, so
  mind the whitespace control here.
#}

{# agg(expr) filter (where cond). Snowflake has no FILTER clause. `gap` (the
   whitespace before `filter`) and `head` (`filter` through the space after
   `where`) keep the original spelling, so the text stays byte-identical. #}
{% macro dagbench_agg_filter(agg, expr, cond, gap=' ', head='filter (where ') -%}
{%- if target.type == 'snowflake' -%}
  {%- set e = expr | trim -%}
  {%- if e == '*' -%}
{{ agg }}(case when {{ cond }} then 1 end)
  {%- elif e[:9] | lower == 'distinct ' -%}
{{ agg }}(distinct case when {{ cond }} then {{ e[9:] | trim }} end)
  {%- else -%}
{{ agg }}(case when {{ cond }} then {{ e }} end)
  {%- endif -%}
{%- else -%}
{{ agg }}({{ expr }}){{ gap }}{{ head }}{{ cond }})
{%- endif -%}
{%- endmacro %}

{#
  Function names that differ on Snowflake; arguments stay as written:
    {{ dagbench_fn('least') }}(a, b)
  Snowflake's least/greatest return NULL when any argument is NULL; DuckDB and
  Postgres skip NULLs, which is what the *_ignore_nulls variants do.
#}
{% macro dagbench_fn(name) -%}
{%- if target.type == 'snowflake' -%}
{{ {'least': 'least_ignore_nulls',
    'greatest': 'greatest_ignore_nulls',
    'bool_or': 'boolor_agg',
    'bool_and': 'booland_agg'}.get(name | lower, name) }}
{%- else -%}
{{ name }}
{%- endif -%}
{%- endmacro %}

{# extract('part' from x): Snowflake wants an unquoted date part. #}
{% macro dagbench_extract(part, expr) -%}
{%- if target.type == 'snowflake' -%}
extract({{ part }} from {{ expr }})
{%- else -%}
extract('{{ part }}' from {{ expr }})
{%- endif -%}
{%- endmacro %}

{# extract(epoch from a - b): seconds between two timestamps, fractional.
   Snowflake can't subtract timestamps. #}
{% macro dagbench_epoch_diff(a, b) -%}
{%- if target.type == 'snowflake' -%}
(timestampdiff(microsecond, {{ b }}, {{ a }}) / 1e6)
{%- else -%}
extract(epoch from {{ a }} - {{ b }})
{%- endif -%}
{%- endmacro %}

{#
  Per-session settings on Snowflake, used as a project-wide pre-hook and
  on-run-start: statement timeouts, and
  UTC so current_date/current_timestamp match the DuckDB reference build (the
  account default is America/Los_Angeles), and Snowflake's default week rules
  (week_start = 0: dow 0 = Sunday, ISO weeks) in case the account overrides
  them, which matches DuckDB's dow / week. Renders to nothing on other
  targets, and dbt skips empty hooks.
#}
{% macro dagbench_sf_session() -%}
{%- if target.type == 'snowflake' -%}
alter session set statement_timeout_in_seconds = 120, statement_queued_timeout_in_seconds = 300, timezone = 'UTC', week_start = 0, week_of_year_policy = 0
{%- endif -%}
{%- endmacro %}

{#
  Named windows. Snowflake has no WINDOW clause, so the spec is inlined there:
    {%- set w = "partition by x order by y" -%}
    avg(v) over {{ dagbench_over('w', w) }} ...
    from t
    {{ dagbench_window_clause('w', w) }}
#}
{% macro dagbench_over(name, spec) -%}
{%- if target.type == 'snowflake' -%}
({{ spec }})
{%- else -%}
{{ name }}
{%- endif -%}
{%- endmacro %}

{% macro dagbench_window_clause(name, spec) -%}
{%- if target.type != 'snowflake' -%}
window {{ name }} as ({{ spec }})
{%- endif -%}
{%- endmacro %}

{# A quoted identifier for a column the loader created. Unquoted names are
   upper-cased by Snowflake and the loader creates every column upper-case, so
   a quoted reserved word ("partition") must be upper-case there. #}
{% macro dagbench_ident(name) -%}
{%- if target.type == 'snowflake' -%}
"{{ name | upper }}"
{%- else -%}
"{{ name }}"
{%- endif -%}
{%- endmacro %}

{# cast(<double> as bigint) the DuckDB way: round half to even. Snowflake's
   cast rounds half away from zero. Only used on snowflake branches. #}
{% macro dagbench_bigint_half_even(expr) -%}
{%- set q = "cast(" ~ expr ~ " as double precision)" -%}
cast(case when {{ q }} - floor({{ q }}) = 0.5 then 2 * round({{ q }} / 2) else round({{ q }}) end as bigint)
{%- endmacro %}

{# string_agg(expr, sep order by o): Snowflake spells it
   listagg(expr, sep) within group (order by o). `expr` may start with `distinct`. #}
{% macro dagbench_string_agg(expr, sep, order_by, kw=' order by ') -%}
{%- if target.type == 'snowflake' -%}
listagg({{ expr }}, {{ sep }}) within group (order by {{ order_by }})
{%- else -%}
string_agg({{ expr }}, {{ sep }}{{ kw }}{{ order_by }})
{%- endif -%}
{%- endmacro %}

{# fn(p) within group (order by x) filter (where c), an ordered-set aggregate
   with FILTER. `head` is "fn(p) within group (order by " as written. #}
{% macro dagbench_ordered_set_filter(head, order_expr, cond, gap=' ', fhead='filter (where ') -%}
{%- if target.type == 'snowflake' -%}
{{ head }}case when {{ cond }} then {{ order_expr }} end)
{%- else -%}
{{ head }}{{ order_expr }}){{ gap }}{{ fhead }}{{ cond }})
{%- endif -%}
{%- endmacro %}

{# regexp_replace(s, p, r, 'g'): Postgres needs 'g' to replace every match;
   Snowflake's 4th argument is a position and it replaces every match by
   default. Use as regexp_replace(s, p, r{{ dagbench_regexp_flag_g() }}). #}
{% macro dagbench_regexp_flag_g() -%}
{%- if target.type != 'snowflake' -%}
, 'g'
{%- endif -%}
{%- endmacro %}

{# x similar to 'pattern' (a whole-string match): Snowflake has no SIMILAR TO;
   regexp_like also matches the whole string. Only for patterns without the
   SQL wildcards % and _. #}
{% macro dagbench_similar_to(expr, pattern) -%}
{%- if target.type == 'snowflake' -%}
regexp_like({{ expr }}, {{ pattern }})
{%- else -%}
{{ expr }} similar to {{ pattern }}
{%- endif -%}
{%- endmacro %}

{# regexp_replace(s, p, r) without a flag replaces the first match on DuckDB and
   Postgres; Snowflake replaces every match unless told occurrence 1. Use as
   regexp_replace(s, p, r{{ dagbench_regexp_first_only() }}). #}
{% macro dagbench_regexp_first_only() -%}
{%- if target.type == 'snowflake' -%}
, 1, 1
{%- endif -%}
{%- endmacro %}

{# a - b for timestamps/dates where DuckDB and Postgres give an INTERVAL.
   Snowflake can't subtract timestamps; there it is the difference in seconds
   (fractional), which utils/compare_snowflake.py compares to the interval's
   total seconds. Pair comparisons with dagbench_interval_literal. #}
{% macro dagbench_ts_diff(a, b) -%}
{%- if target.type == 'snowflake' -%}
(timestampdiff(microsecond, {{ b }}, {{ a }}) / 1e6)
{%- else -%}
{{ a }} - {{ b }}
{%- endif -%}
{%- endmacro %}

{# An interval literal compared with dagbench_ts_diff: `seconds` on Snowflake. #}
{% macro dagbench_interval_literal(literal, seconds) -%}
{%- if target.type == 'snowflake' -%}
{{ seconds }}
{%- else -%}
{{ literal }}
{%- endif -%}
{%- endmacro %}
