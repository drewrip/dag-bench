{% test quality_baseline(model, not_null_columns=[], range_checks=[]) %}
{#
    Cheap, single-scan replacement for "not_empty" + one dbt_utils.at_least_one
    per column + one dbt_utils.accepted_range per column.

    p10_energy's models stack several layers of views on top of a large
    stg_readings table, so every separate generic test re-executes the full
    upstream query. Running not_empty and one at_least_one/accepted_range per
    column meant N+1 full recomputations of the same view chain per model.
    This test computes count(*), count(col) for every not_null_columns entry,
    and min(col)/max(col) for every range_checks entry, all in ONE aggregate
    query, then evaluates every condition against that single row.

    not_null_columns: list of column names that must not be entirely NULL.
    range_checks: list of {column, min (optional), max (optional)} dicts.
#}

with agg as (
    select
        count(*) as row_count
        {%- for col in not_null_columns %}
        , count({{ col }}) as not_null_cnt_{{ loop.index }}
        {%- endfor %}
        {%- for rc in range_checks %}
        {%- if rc.get('min') is not none %}
        , min({{ rc.column }}) as min_{{ loop.index }}
        {%- endif %}
        {%- if rc.get('max') is not none %}
        , max({{ rc.column }}) as max_{{ loop.index }}
        {%- endif %}
        {%- endfor %}
    from {{ model }}
)

select 1 as issue
from agg
where row_count = 0
{%- for col in not_null_columns %}
    or not_null_cnt_{{ loop.index }} = 0
{%- endfor %}
{%- for rc in range_checks %}
    {%- if rc.get('min') is not none %}
    or min_{{ loop.index }} < {{ rc.min }}
    {%- endif %}
    {%- if rc.get('max') is not none %}
    or max_{{ loop.index }} > {{ rc.max }}
    {%- endif %}
{%- endfor %}

{% endtest %}
