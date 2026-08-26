select dept_name, division, location,
    count(*) filter (where is_active) as active_hc, count(*) as total_hc,
    count(*) filter (where gender='female') as female_count,
    round(cast(avg(tenure_years) as numeric(38,10)), 2) as avg_tenure
from {{ ref('stg_employees') }}
group by dept_name, division, location
having count(*) > 0
