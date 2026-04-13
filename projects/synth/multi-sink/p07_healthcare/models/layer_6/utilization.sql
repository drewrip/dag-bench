select plan_type, members, paid, round(paid/nullif(members,0),2) as pmpm,
    high_cost_members, round(high_cost_members*100.0/nullif(members,0),2) as high_cost_pct
from {{ ref('plan_cost') }}
