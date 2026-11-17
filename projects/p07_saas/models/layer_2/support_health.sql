select account_id, count(*) as total_tickets,
    {{ dagbench_agg_filter('count', '*', 'is_urgent') }} as urgent_tickets,
    {{ dagbench_agg_filter('count', '*', 'is_resolved') }} as resolved,
    cast(round({{ dagbench_agg_filter('avg', 'csat_score', 'csat_score is not null') }}, 2) as numeric(10,2)) as avg_csat,
    cast(round(cast({{ dagbench_agg_filter('avg', 'ttr_hours', 'is_resolved') }} as numeric(18,3)), 1) as numeric(10,1)) as avg_ttr
from {{ ref('stg_tickets') }}
group by account_id
having {{ dagbench_agg_filter('count', '*', 'is_urgent') }} < count(*)
