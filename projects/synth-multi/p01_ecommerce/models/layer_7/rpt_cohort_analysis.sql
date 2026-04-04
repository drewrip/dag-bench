select signup_year, value_segment,
    sum(customers) as customers,
    round(sum(cohort_revenue),2) as revenue,
    round(avg(avg_aov),2) as avg_aov,
    current_timestamp as generated_at
from {{ ref('cohort_revenue') }}
group by signup_year, value_segment
order by signup_year desc, value_segment
