select plan_type, members, paid, round(cast(paid/nullif(members,0) as numeric(38,10)), 2) as pmpm,
    high_cost_members, round(cast(high_cost_members*100.0/nullif(members,0) as numeric(38,10)), 2) as high_cost_pct
from {{ ref('plan_cost') }}
