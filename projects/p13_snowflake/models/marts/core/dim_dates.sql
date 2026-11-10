{{
    config(
        materialized='table',
        tags=['dimension', 'core']
    )
}}

-- Date dimension generated from sales history
with date_spine as (
    select distinct order_date::date as date_value
    from {{ ref('fct_sales') }}
    where order_date is not null
),

date_attributes as (
    select
        date_value,
        extract(year from date_value) as year,
        extract(quarter from date_value) as quarter,
        extract(month from date_value) as month,
        extract(week from date_value) as week,
        extract(day from date_value) as day,
        {{ dagbench_dayofweek("date_value") }} as day_of_week,
        {{ dagbench_dayname("date_value") }} as day_name,
        {{ dagbench_monthname("date_value") }} as month_name,
        case when {{ dagbench_dayofweek("date_value") }} in (6, 7) then true else false end as is_weekend,
        date_trunc('month', date_value) as month_start_date,
        {{ dagbench_last_day("date_value") }} as month_end_date
    from date_spine
)

select * from date_attributes
order by date_value
