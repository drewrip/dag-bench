-- depends_on: {{ ref('stripe__balance_transactions') }}
with spine as (

    {# dag-bench: fixed window instead of the data's min date .. today #}
    {% set first_date = "cast('" ~ var('dagbench_start_date') ~ "' as date)" %}
    {% set last_date = "cast('" ~ var('dagbench_as_of_date') ~ "' as date)" %}

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date=first_date,
        end_date=dbt.dateadd("day", 1, last_date)
    ) }}

),

account as (
    select *
    from {{ ref('stg_stripe__account') }}
),

date_spine as (
    select
        cast({{ dbt.date_trunc("day", "date_day") }} as date) as date_day, 
        cast({{ dbt.date_trunc("week", "date_day") }} as date) as date_week, 
        cast({{ dbt.date_trunc("month", "date_day") }} as date) as date_month,
        cast({{ dbt.date_trunc("year", "date_day") }} as date) as date_year,  
        row_number() over (order by cast({{ dbt.date_trunc("day", "date_day") }} as date)) as date_index
    from spine
),

final as (
    select distinct
        account.account_id,
        account.source_relation,
        date_spine.date_day,
        date_spine.date_week,
        date_spine.date_month,
        date_spine.date_year,
        date_spine.date_index
    from account 
    cross join date_spine
)

select * 
from final
