select emp_id, sum(leave_days) as total_leave_days,
    {{ dagbench_agg_filter('sum', 'leave_days', "leave_type='sick'") }}   as sick_days,
    {{ dagbench_agg_filter('sum', 'leave_days', "leave_type='vacation'") }} as annual_days,
    {{ dagbench_agg_filter('count', '*', 'not approved') }} as unapproved
from {{ ref('stg_leaves') }}
group by emp_id
