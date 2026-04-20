select dept_name, division, location,
    count(*) filter (where is_active)               as active_headcount,
    count(*)                                        as total_headcount,
    count(*) filter (where gender='F')              as female_count,
    count(*) filter (where gender='M')              as male_count,
    round(CAST(avg(tenure_years) AS NUMERIC), 2)                      as avg_tenure
from {{ ref('stg_employees') }}
group by dept_name, division, location
