-- Final materialised table combining category and cohort insights
with cat as (
    select category_revenue_rank, category_name,
        top10_revenue as cat_revenue,
        avg_rating, avg_pos_pct
    from {{ ref('executive_category_view') }}
    where category_revenue_rank <= 5
),
cohort as (
    select signup_year, value_segment,
        sum(customers)     as customers,
        sum(cohort_revenue) as revenue
    from {{ ref('cohort_revenue') }}
    group by signup_year, value_segment
)
select
    cohort.signup_year,
    cohort.value_segment,
    cohort.customers,
    cohort.revenue                           as cohort_revenue,
    cat.category_name                        as top_category,
    cat.cat_revenue,
    cat.avg_rating,
    round(cohort.revenue / nullif(cohort.customers,0), 2) as rev_per_customer,
    current_timestamp                        as generated_at
from cohort
cross join cat
order by cohort.signup_year desc, cohort.value_segment, cat.category_revenue_rank
