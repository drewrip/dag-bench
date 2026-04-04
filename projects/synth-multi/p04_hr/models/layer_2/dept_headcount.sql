select dept_name, division, location,
    count(*) filter (where is_active) as active_hc, count(*) as total_hc,
    count(*) filter (where gender='F') as female_count,
    round(avg(tenure_years),2) as avg_tenure
from {{ ref('stg_employees') }}
group by dept_name, division, location
