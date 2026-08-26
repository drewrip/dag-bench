select signup_year, value_segment,
    sum(customers) as customers,
    round(cast(sum(cohort_revenue) as numeric(38,10)), 2) as revenue,
    round(cast(avg(avg_aov) as numeric(38,10)), 2) as avg_aov,
    current_timestamp as generated_at
from {{ ref('cohort_revenue') }}
group by signup_year, value_segment
order by signup_year desc nulls last, value_segment
