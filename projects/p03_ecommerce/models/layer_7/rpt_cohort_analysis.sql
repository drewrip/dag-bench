select signup_year, value_segment,
    cast(sum(customers) as bigint) as customers,
    cast(round(sum(cohort_revenue), 2) as numeric(18,2)) as revenue,
    cast(round(avg(avg_aov), 2) as numeric(18,2)) as avg_aov,
    current_timestamp as generated_at
from {{ ref('cohort_revenue') }}
group by signup_year, value_segment
order by signup_year desc nulls last, value_segment
