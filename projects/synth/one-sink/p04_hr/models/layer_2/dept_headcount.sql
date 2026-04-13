select dept_name, division, location,
    count(*) filter (where is_active)               as active_headcount,
    count(*)                                        as total_headcount,
    count(*) filter (where gender='F')              as female_count,
    count(*) filter (where gender='M')              as male_count,
    round(avg(tenure_years),2)                      as avg_tenure
from {{ ref('stg_employees') }}
group by dept_name, division, location
